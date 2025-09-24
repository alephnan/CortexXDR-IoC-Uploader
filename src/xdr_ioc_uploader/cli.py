from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import typer
from rich.console import Console
from rich.table import Table

from . import file_ops
from .config import Settings, get_settings
from .csv_io import load_csv_rows
from .models import IndicatorRow, IndicatorType
from .multi_tenant_config import get_multi_tenant_settings
from .multi_tenant_uploader import MultiTenantUploader
from .reporting import emit_run_artifact, write_errors_csv, emit_multi_tenant_artifact, write_multi_tenant_errors_csv
from .transformers import build_csv_request_data, build_json_objects
from .uploader import Uploader, UploadMode
from .api_client import XdrApiClient


app = typer.Typer(add_completion=False, help="Validate and upload IOC CSV/JSON files to Cortex XDR")
console = Console()


def _format_summary_value(value: object) -> str:
	if isinstance(value, dict):
		if not value:
			return "-"
		return ", ".join(f"{k}={v}" for k, v in sorted(value.items()))
	return str(value)


def _print_operation_summary(title: str, summary: Dict[str, object]) -> None:
	table = Table(title=title, show_header=True, header_style="bold cyan")
	table.add_column("Metric", style="cyan", no_wrap=True)
	table.add_column("Value", style="white")
	for key, value in summary.items():
		table.add_row(str(key), _format_summary_value(value))
	console.print(table)


def _resolve_override_value(
	default_value: str,
	use_default: bool,
	explicit_value: Optional[str],
) -> Optional[str]:
	if explicit_value is not None:
		return explicit_value
	if use_default:
		return default_value
	return None


def _build_override_map(
	default_value: str,
	*,
	hash_default: bool,
	hash_value: Optional[str],
	ip_default: bool,
	ip_value: Optional[str],
	domain_default: bool,
	domain_value: Optional[str],
	path_default: bool,
	path_value: Optional[str],
	filename_default: bool,
	filename_value: Optional[str],
) -> Tuple[Dict[str, Optional[str]], bool]:
	default_flags = {
		IndicatorType.HASH.value: hash_default,
		IndicatorType.IP.value: ip_default,
		IndicatorType.DOMAIN_NAME.value: domain_default,
		IndicatorType.PATH.value: path_default,
		IndicatorType.FILENAME.value: filename_default,
	}
	explicit_values = {
		IndicatorType.HASH.value: hash_value,
		IndicatorType.IP.value: ip_value,
		IndicatorType.DOMAIN_NAME.value: domain_value,
		IndicatorType.PATH.value: path_value,
		IndicatorType.FILENAME.value: filename_value,
	}
	overrides: Dict[str, Optional[str]] = {}
	for type_value, explicit in explicit_values.items():
		if explicit is not None:
			overrides[type_value] = explicit
	for type_value, use_default in default_flags.items():
		if use_default and type_value not in overrides:
			overrides[type_value] = default_value
	apply_default_globally = not any(default_flags.values())
	return overrides, apply_default_globally


def _resolve_output_path(
	file_path: Path,
	output: Optional[Path],
	in_place: bool,
	command_name: str,
) -> Path:
	if in_place and output is not None:
		raise typer.BadParameter("Cannot use --output with --in-place.", param_hint="--output")

	target = file_path if in_place else output or file_ops.resolve_default_output(file_path, command_name)

	if not in_place:
		try:
			if target.resolve() == file_path.resolve():
				raise typer.BadParameter(
					"Output path matches input; use --in-place instead.",
					param_hint="--output",
				)
		except FileNotFoundError:
			pass

	return target


def _write_operation_output(
	*,
	command_name: str,
	file_path: Path,
	output: Optional[Path],
	in_place: bool,
	no_backup: bool,
	dry_run: bool,
	encoding: str,
	rows: List[IndicatorRow],
) -> None:
	if no_backup and not in_place:
		raise typer.BadParameter(
			"--no-backup can only be used with --in-place.",
			param_hint="--no-backup",
		)
	if dry_run:
		console.print("[yellow]Dry-run: no files written.[/yellow]")
		return

	target = _resolve_output_path(file_path, output, in_place, command_name)
	backup_path: Optional[Path] = None
	if in_place and not no_backup:
		backup_path = file_ops.create_backup(file_path)
		console.print(f"[yellow]Backup created: {backup_path}[/yellow]")

	file_ops.write_rows(rows, target, encoding)
	console.print(f"[green]Wrote {len(rows)} rows to {target}[/green]")
	if not in_place:
		console.print(f"[blue]Output file: {target}[/blue]")
	if backup_path:
		console.print(f"[blue]Original preserved at: {backup_path}[/blue]")


def _print_summary(summary: dict) -> None:
	table = Table(show_header=True, header_style="bold magenta")
	table.add_column("Metric")
	table.add_column("Value")
	for key, value in summary.items():
		table.add_row(str(key), str(value))
	console.print(table)


@app.command(name="file-classify")
def file_classify(
	file: Path = typer.Argument(
		...,
		exists=True,
		readable=True,
		dir_okay=False,
		help="Path to the IOC CSV file",
	),
	output: Optional[Path] = typer.Option(
		None,
		"--output",
		"-o",
		help="Write results to this CSV file (defaults to <name>-classify.csv).",
	),
	in_place: bool = typer.Option(False, "--in-place", help="Overwrite the input file with results."),
	no_backup: bool = typer.Option(False, "--no-backup", help="Skip creating a .bak when using --in-place."),
	only_empty: bool = typer.Option(False, "--only-empty", help="Only set type when the column is empty."),
	force: bool = typer.Option(False, "--force", help="Overwrite existing types even when they differ."),
	dry_run: bool = typer.Option(False, "--dry-run", help="Preview changes without writing any file."),
):
	try:
		rows, encoding, original_types = file_ops.load_rows_for_classification(file)
		result = file_ops.classify_rows(
			rows,
			only_empty=only_empty,
			force=force,
			original_types=original_types,
		)
		_print_operation_summary("Classification Summary", result.summary)
		_write_operation_output(
			command_name="file-classify",
			file_path=file,
			output=output,
			in_place=in_place,
			no_backup=no_backup,
			dry_run=dry_run,
			encoding=encoding,
			rows=result.rows,
		)
	except typer.BadParameter:
		raise
	except Exception as exc:
		console.print(f"[red]File classification failed: {exc}[/red]")
		raise typer.Exit(code=1)


@app.command(name="file-reputation")
def file_reputation(
	value: str = typer.Argument(
		...,
		metavar="VALUE",
		help="Default reputation to assign (bad, good, suspicious, unknown, no reputation).",
	),
	file: Path = typer.Argument(
		...,
		exists=True,
		readable=True,
		dir_okay=False,
		help="Path to the IOC CSV file",
	),
	output: Optional[Path] = typer.Option(
		None,
		"--output",
		"-o",
		help="Write results to this CSV file (defaults to <name>-reputation.csv).",
	),
	in_place: bool = typer.Option(False, "--in-place", help="Overwrite the input file with results."),
	no_backup: bool = typer.Option(False, "--no-backup", help="Skip creating a .bak when using --in-place."),
	only_empty: bool = typer.Option(False, "--only-empty", help="Only update rows where reputation is empty."),
	dry_run: bool = typer.Option(False, "--dry-run", help="Preview changes without writing any file."),
	hash_default: bool = typer.Option(
		False,
		"--hash",
		help="Apply the command default to HASH indicators.",
	),
	hash_value: Optional[str] = typer.Option(
		None,
		"--hash-value",
		help="Explicit override value for HASH indicators.",
		metavar="VALUE",
	),
	ip_default: bool = typer.Option(
		False,
		"--ip",
		help="Apply the command default to IP indicators.",
	),
	ip_value: Optional[str] = typer.Option(
		None,
		"--ip-value",
		help="Explicit override value for IP indicators.",
		metavar="VALUE",
	),
	domain_default: bool = typer.Option(
		False,
		"--domain",
		help="Apply the command default to DOMAIN_NAME indicators.",
	),
	domain_value: Optional[str] = typer.Option(
		None,
		"--domain-value",
		help="Explicit override value for DOMAIN_NAME indicators.",
		metavar="VALUE",
	),
	path_default: bool = typer.Option(
		False,
		"--path",
		help="Apply the command default to PATH indicators.",
	),
	path_value: Optional[str] = typer.Option(
		None,
		"--path-value",
		help="Explicit override value for PATH indicators.",
		metavar="VALUE",
	),
	filename_default: bool = typer.Option(
		False,
		"--filename",
		help="Apply the command default to FILENAME indicators.",
	),
	filename_value: Optional[str] = typer.Option(
		None,
		"--filename-value",
		help="Explicit override value for FILENAME indicators.",
		metavar="VALUE",
	),
):
	overrides, apply_globally = _build_override_map(
		value,
		hash_default=hash_default,
		hash_value=hash_value,
		ip_default=ip_default,
		ip_value=ip_value,
		domain_default=domain_default,
		domain_value=domain_value,
		path_default=path_default,
		path_value=path_value,
		filename_default=filename_default,
		filename_value=filename_value,
	)
	try:
		rows, encoding = file_ops.load_rows(file)
		result = file_ops.apply_reputation(
			rows,
			value,
			overrides,
			only_empty=only_empty,
			apply_default_globally=apply_globally,
		)
		_print_operation_summary("Reputation Update", result.summary)
		_write_operation_output(
			command_name="file-reputation",
			file_path=file,
			output=output,
			in_place=in_place,
			no_backup=no_backup,
			dry_run=dry_run,
			encoding=encoding,
			rows=result.rows,
		)
	except file_ops.FileOperationError as exc:
		raise typer.BadParameter(str(exc))
	except typer.BadParameter:
		raise
	except Exception as exc:
		console.print(f"[red]File reputation update failed: {exc}[/red]")
		raise typer.Exit(code=1)


@app.command(name="file-severity")
def file_severity(
	value: str = typer.Argument(
		...,
		metavar="VALUE",
		help="Default severity to assign (high, medium, low, critical, informational).",
	),
	file: Path = typer.Argument(
		...,
		exists=True,
		readable=True,
		dir_okay=False,
		help="Path to the IOC CSV file",
	),
	output: Optional[Path] = typer.Option(
		None,
		"--output",
		"-o",
		help="Write results to this CSV file (defaults to <name>-severity.csv).",
	),
	in_place: bool = typer.Option(False, "--in-place", help="Overwrite the input file with results."),
	no_backup: bool = typer.Option(False, "--no-backup", help="Skip creating a .bak when using --in-place."),
	only_empty: bool = typer.Option(False, "--only-empty", help="Only update rows where severity is empty."),
	dry_run: bool = typer.Option(False, "--dry-run", help="Preview changes without writing any file."),
	hash_default: bool = typer.Option(
		False,
		"--hash",
		help="Apply the command default to HASH indicators.",
	),
	hash_value: Optional[str] = typer.Option(
		None,
		"--hash-value",
		help="Explicit override value for HASH indicators.",
		metavar="VALUE",
	),
	ip_default: bool = typer.Option(
		False,
		"--ip",
		help="Apply the command default to IP indicators.",
	),
	ip_value: Optional[str] = typer.Option(
		None,
		"--ip-value",
		help="Explicit override value for IP indicators.",
		metavar="VALUE",
	),
	domain_default: bool = typer.Option(
		False,
		"--domain",
		help="Apply the command default to DOMAIN_NAME indicators.",
	),
	domain_value: Optional[str] = typer.Option(
		None,
		"--domain-value",
		help="Explicit override value for DOMAIN_NAME indicators.",
		metavar="VALUE",
	),
	path_default: bool = typer.Option(
		False,
		"--path",
		help="Apply the command default to PATH indicators.",
	),
	path_value: Optional[str] = typer.Option(
		None,
		"--path-value",
		help="Explicit override value for PATH indicators.",
		metavar="VALUE",
	),
	filename_default: bool = typer.Option(
		False,
		"--filename",
		help="Apply the command default to FILENAME indicators.",
	),
	filename_value: Optional[str] = typer.Option(
		None,
		"--filename-value",
		help="Explicit override value for FILENAME indicators.",
		metavar="VALUE",
	),
):
	overrides, apply_globally = _build_override_map(
		value,
		hash_default=hash_default,
		hash_value=hash_value,
		ip_default=ip_default,
		ip_value=ip_value,
		domain_default=domain_default,
		domain_value=domain_value,
		path_default=path_default,
		path_value=path_value,
		filename_default=filename_default,
		filename_value=filename_value,
	)
	try:
		rows, encoding = file_ops.load_rows(file)
		result = file_ops.apply_severity(
			rows,
			value,
			overrides,
			only_empty=only_empty,
			apply_default_globally=apply_globally,
		)
		_print_operation_summary("Severity Update", result.summary)
		_write_operation_output(
			command_name="file-severity",
			file_path=file,
			output=output,
			in_place=in_place,
			no_backup=no_backup,
			dry_run=dry_run,
			encoding=encoding,
			rows=result.rows,
		)
	except file_ops.FileOperationError as exc:
		raise typer.BadParameter(str(exc))
	except typer.BadParameter:
		raise
	except Exception as exc:
		console.print(f"[red]File severity update failed: {exc}[/red]")
		raise typer.Exit(code=1)


@app.command(name="file-comment")
def file_comment(
	text: str = typer.Argument(..., metavar="TEXT", help="Comment text to assign to the comment column."),
	file: Path = typer.Argument(
		...,
		exists=True,
		readable=True,
		dir_okay=False,
		help="Path to the IOC CSV file",
	),
	output: Optional[Path] = typer.Option(
		None,
		"--output",
		"-o",
		help="Write results to this CSV file (defaults to <name>-comment.csv).",
	),
	in_place: bool = typer.Option(False, "--in-place", help="Overwrite the input file with results."),
	no_backup: bool = typer.Option(False, "--no-backup", help="Skip creating a .bak when using --in-place."),
	only_empty: bool = typer.Option(False, "--only-empty", help="Only update rows where comment is empty."),
	dry_run: bool = typer.Option(False, "--dry-run", help="Preview changes without writing any file."),
	hash_default: bool = typer.Option(
		False,
		"--hash",
		help="Apply the command default comment to HASH indicators.",
	),
	hash_value: Optional[str] = typer.Option(
		None,
		"--hash-value",
		help="Explicit comment for HASH indicators.",
		metavar="VALUE",
	),
	ip_default: bool = typer.Option(
		False,
		"--ip",
		help="Apply the command default comment to IP indicators.",
	),
	ip_value: Optional[str] = typer.Option(
		None,
		"--ip-value",
		help="Explicit comment for IP indicators.",
		metavar="VALUE",
	),
	domain_default: bool = typer.Option(
		False,
		"--domain",
		help="Apply the command default comment to DOMAIN_NAME indicators.",
	),
	domain_value: Optional[str] = typer.Option(
		None,
		"--domain-value",
		help="Explicit comment for DOMAIN_NAME indicators.",
		metavar="VALUE",
	),
	path_default: bool = typer.Option(
		False,
		"--path",
		help="Apply the command default comment to PATH indicators.",
	),
	path_value: Optional[str] = typer.Option(
		None,
		"--path-value",
		help="Explicit comment for PATH indicators.",
		metavar="VALUE",
	),
	filename_default: bool = typer.Option(
		False,
		"--filename",
		help="Apply the command default comment to FILENAME indicators.",
	),
	filename_value: Optional[str] = typer.Option(
		None,
		"--filename-value",
		help="Explicit comment for FILENAME indicators.",
		metavar="VALUE",
	),
):
	overrides, apply_globally = _build_override_map(
		text,
		hash_default=hash_default,
		hash_value=hash_value,
		ip_default=ip_default,
		ip_value=ip_value,
		domain_default=domain_default,
		domain_value=domain_value,
		path_default=path_default,
		path_value=path_value,
		filename_default=filename_default,
		filename_value=filename_value,
	)
	try:
		rows, encoding = file_ops.load_rows(file)
		result = file_ops.apply_comment(
			rows,
			text,
			overrides,
			only_empty=only_empty,
			apply_default_globally=apply_globally,
		)
		_print_operation_summary("Comment Update", result.summary)
		_write_operation_output(
			command_name="file-comment",
			file_path=file,
			output=output,
			in_place=in_place,
			no_backup=no_backup,
			dry_run=dry_run,
			encoding=encoding,
			rows=result.rows,
		)
	except file_ops.FileOperationError as exc:
		raise typer.BadParameter(str(exc))
	except typer.BadParameter:
		raise
	except Exception as exc:
		console.print(f"[red]File comment update failed: {exc}[/red]")
		raise typer.Exit(code=1)


@app.command(name="file-reliability")
def file_reliability(
	value: str = typer.Argument(
		...,
		metavar="VALUE",
		help="Default reliability to assign (A, B, C, D, E, F, G).",
	),
	file: Path = typer.Argument(
		...,
		exists=True,
		readable=True,
		dir_okay=False,
		help="Path to the IOC CSV file",
	),
	output: Optional[Path] = typer.Option(
		None,
		"--output",
		"-o",
		help="Write results to this CSV file (defaults to <name>-reliability.csv).",
	),
	in_place: bool = typer.Option(False, "--in-place", help="Overwrite the input file with results."),
	no_backup: bool = typer.Option(False, "--no-backup", help="Skip creating a .bak when using --in-place."),
	only_empty: bool = typer.Option(False, "--only-empty", help="Only update rows where reliability is empty."),
	dry_run: bool = typer.Option(False, "--dry-run", help="Preview changes without writing any file."),
	hash_default: bool = typer.Option(
		False,
		"--hash",
		help="Apply the command default to HASH indicators.",
	),
	hash_value: Optional[str] = typer.Option(
		None,
		"--hash-value",
		help="Explicit override value for HASH indicators.",
		metavar="VALUE",
	),
	ip_default: bool = typer.Option(
		False,
		"--ip",
		help="Apply the command default to IP indicators.",
	),
	ip_value: Optional[str] = typer.Option(
		None,
		"--ip-value",
		help="Explicit override value for IP indicators.",
		metavar="VALUE",
	),
	domain_default: bool = typer.Option(
		False,
		"--domain",
		help="Apply the command default to DOMAIN_NAME indicators.",
	),
	domain_value: Optional[str] = typer.Option(
		None,
		"--domain-value",
		help="Explicit override value for DOMAIN_NAME indicators.",
		metavar="VALUE",
	),
	path_default: bool = typer.Option(
		False,
		"--path",
		help="Apply the command default to PATH indicators.",
	),
	path_value: Optional[str] = typer.Option(
		None,
		"--path-value",
		help="Explicit override value for PATH indicators.",
		metavar="VALUE",
	),
	filename_default: bool = typer.Option(
		False,
		"--filename",
		help="Apply the command default to FILENAME indicators.",
	),
	filename_value: Optional[str] = typer.Option(
		None,
		"--filename-value",
		help="Explicit override value for FILENAME indicators.",
		metavar="VALUE",
	),
):
	overrides, apply_globally = _build_override_map(
		value,
		hash_default=hash_default,
		hash_value=hash_value,
		ip_default=ip_default,
		ip_value=ip_value,
		domain_default=domain_default,
		domain_value=domain_value,
		path_default=path_default,
		path_value=path_value,
		filename_default=filename_default,
		filename_value=filename_value,
	)
	try:
		rows, encoding = file_ops.load_rows(file)
		result = file_ops.apply_reliability(
			rows,
			value,
			overrides,
			only_empty=only_empty,
			apply_default_globally=apply_globally,
		)
		_print_operation_summary("Reliability Update", result.summary)
		_write_operation_output(
			command_name="file-reliability",
			file_path=file,
			output=output,
			in_place=in_place,
			no_backup=no_backup,
			dry_run=dry_run,
			encoding=encoding,
			rows=result.rows,
		)
	except file_ops.FileOperationError as exc:
		raise typer.BadParameter(str(exc))
	except typer.BadParameter:
		raise
	except Exception as exc:
		console.print(f"[red]File reliability update failed: {exc}[/red]")
		raise typer.Exit(code=1)


@app.command()
def validate(
	file: Path = typer.Argument(..., exists=True, readable=True, help="Path to the IOC CSV file"),
	mode: UploadMode = typer.Option(UploadMode.csv, case_sensitive=False, help="Validation mode (csv/json)"),
):
	"""Validate a CSV/JSON file structure and format (offline validation only)."""
	console.print("[blue]Validating file structure and format (no network calls)[/blue]")
	rows = load_csv_rows(file, mode=mode)
	
	# Basic validation - if load_csv_rows succeeds, the structure is valid
	if mode == UploadMode.csv:
		request_data = build_csv_request_data(rows)
		console.print("[green]✓ CSV structure valid[/green]")
	else:
		objects = build_json_objects(rows)
		console.print("[green]✓ JSON structure valid[/green]")
	
	summary = {
		"total_rows": len(rows),
		"errors": 0,
		"endpoint": "insert_csv" if mode == UploadMode.csv else "insert_jsons",
		"validated": True,
		"offline_mode": True,
	}
	reply = {"success": True, "offline_validation": True}
	
	_print_summary(summary)
	artifact_path = emit_run_artifact(action="validate", payload={"summary": summary, "reply": reply})
	console.print(f"Artifact: {artifact_path}")


@app.command()
def upload(
	file: Path = typer.Argument(..., exists=True, readable=True, help="Path to the IOC CSV file"),
	mode: UploadMode = typer.Option(UploadMode.csv, case_sensitive=False, help="Upload mode (csv/json)"),
	batch_size: int = typer.Option(1000, min=1, help="Rows per request for commit phase"),
):
	"""Upload a CSV/JSON file to Cortex XDR."""
	settings: Settings = get_settings()
	rows = load_csv_rows(file, mode=mode)
	uploader = Uploader(settings=settings)

	# Commit flow: validate entire dataset first; if clean, upload in batches
	if mode == UploadMode.csv:
		request_data_all = build_csv_request_data(rows)
		validation = uploader.validate_csv(request_data_all)
	else:
		objects_all = build_json_objects(rows)
		validation = uploader.validate_json(objects_all)

	errors = validation.get("errors") or validation.get("validation_errors") or []
	if errors:
		console.print("[red]Validation errors. Not committing.[/red]")
		write_errors_csv(errors)
		emit_run_artifact(action="upload-validation-failed", payload={"reply": validation})
		raise typer.Exit(code=2)

	# Commit in batches
	if mode == UploadMode.csv:
		reply = uploader.commit_csv(rows, batch_size=batch_size)
	else:
		reply = uploader.commit_json(rows, batch_size=batch_size)

	summary = {
		"total_rows": len(rows),
		"committed": reply.get("succeeded", 0),
		"failed": reply.get("failed", 0),
		"endpoint": "insert_csv" if mode == UploadMode.csv else "insert_jsons",
	}
	_print_summary(summary)
	emit_run_artifact(action="upload", payload={"summary": summary, "reply": reply})

	if summary["failed"] > 0:
		errs = reply.get("errors") or reply.get("failed_rows") or []
		if isinstance(errs, list) and errs:
			write_errors_csv(errs)
		raise typer.Exit(code=2)


@app.command()
def test_auth():
	"""Test authentication with XDR API using current credentials."""
	try:
		settings: Settings = get_settings()
		console.print(f"[blue]Testing authentication to {settings.fqdn}[/blue]")
		console.print(f"[blue]Using API Key ID: {settings.api_key_id}[/blue]")
		console.print(f"[blue]Advanced auth: {settings.advanced}[/blue]")
		
		client = XdrApiClient(settings)
		
		with console.status("[bold green]Testing authentication...", spinner="dots"):
			response = client.test_authentication()
		
		console.print("[green]✅ Authentication successful![/green]")
		
		# Display response details
		table = Table(show_header=True, header_style="bold magenta")
		table.add_column("Response Field")
		table.add_column("Value")
		
		for key, value in response.items():
			if key == "validation_errors" and isinstance(value, list):
				table.add_row(key, f"{len(value)} errors" if value else "No errors")
			else:
				table.add_row(key, str(value)[:100] + "..." if len(str(value)) > 100 else str(value))
		
		console.print(table)
		
		# Create a report
		artifact_path = emit_run_artifact(action="test-auth", payload={"success": True, "response": response})
		console.print(f"Report saved: {artifact_path}")
		
	except RuntimeError as e:
		console.print(f"[red]❌ Configuration error: {e}[/red]")
		raise typer.Exit(code=1)
	except Exception as e:
		console.print(f"[red]❌ Authentication failed: {e}[/red]")
		console.print("[yellow]💡 Check your credentials in the .env file:[/yellow]")
		console.print("   - XDR_FQDN (without https://)")
		console.print("   - XDR_API_KEY_ID")
		console.print("   - XDR_API_KEY")
		console.print("   - XDR_ADVANCED (true/false)")
		raise typer.Exit(code=1)


@app.command()
def validate_multi(
	file: Path = typer.Argument(..., exists=True, readable=True, help="Path to the IOC CSV file"),
	mode: UploadMode = typer.Option(UploadMode.csv, case_sensitive=False, help="Validation mode (csv/json)"),
	config_file: Optional[Path] = typer.Option(None, help="Path to tenant configuration JSON file"),
	tenants: Optional[str] = typer.Option(None, help="Comma-separated list of tenant names to validate against"),
	max_workers: int = typer.Option(5, min=1, max=20, help="Maximum concurrent tenant validations"),
):
	"""Validate a CSV/JSON file against multiple XDR tenants."""
	try:
		settings = get_multi_tenant_settings(config_file)
		console.print(f"[blue]Loaded configuration for {len(settings.tenants)} tenants[/blue]")
		
		tenant_names = None
		if tenants:
			tenant_names = [name.strip() for name in tenants.split(",")]
			console.print(f"[blue]Validating against selected tenants: {', '.join(tenant_names)}[/blue]")
		
		rows = load_csv_rows(file, mode=mode)
		uploader = MultiTenantUploader(settings, max_workers=max_workers)
		
		console.print(f"[blue]Validating {len(rows)} rows against tenants...[/blue]")
		result = uploader.validate_all(rows, mode, tenant_names)
		
		uploader.print_summary(result)
		
		# Generate reports
		payload = {
			"timestamp": datetime.now().isoformat(),
			"action": "validate-multi",
			"file": str(file),
			"mode": mode.value,
			"total_rows": len(rows),
			"tenant_results": [
				{
					"tenant_name": r.tenant_name,
					"success": r.success,
					"total_rows": r.total_rows,
					"validation_errors": r.validation_errors,
					"error_message": r.error_message
				}
				for r in result.tenant_results
			],
			"summary": {
				"total_tenants": result.total_tenants,
				"successful_tenants": result.successful_tenants,
				"failed_tenants": result.failed_tenants,
				"overall_success": result.overall_success
			}
		}
		
		artifacts = emit_multi_tenant_artifact("validate-multi", payload)
		console.print(f"[blue]Reports saved: {len(artifacts)} files[/blue]")
		
		# Write error CSV files for tenants with errors
		if not result.overall_success:
			error_paths = write_multi_tenant_errors_csv(payload["tenant_results"])
			if error_paths:
				console.print(f"[yellow]Error CSV files: {len(error_paths)} files[/yellow]")
		
		if not result.overall_success:
			raise typer.Exit(code=2)
			
	except Exception as e:
		console.print(f"[red]❌ Multi-tenant validation failed: {e}[/red]")
		raise typer.Exit(code=1)


@app.command()
def upload_multi(
	file: Path = typer.Argument(..., exists=True, readable=True, help="Path to the IOC CSV file"),
	mode: UploadMode = typer.Option(UploadMode.csv, case_sensitive=False, help="Upload mode (csv/json)"),
	batch_size: int = typer.Option(1000, min=1, help="Rows per request for commit phase"),
	config_file: Optional[Path] = typer.Option(None, help="Path to tenant configuration JSON file"),
	tenants: Optional[str] = typer.Option(None, help="Comma-separated list of tenant names to upload to"),
	max_workers: int = typer.Option(5, min=1, max=20, help="Maximum concurrent tenant uploads"),
	skip_validation: bool = typer.Option(False, help="Skip validation phase (not recommended)"),
):
	"""Upload a CSV/JSON file to multiple XDR tenants."""
	try:
		settings = get_multi_tenant_settings(config_file)
		console.print(f"[blue]Loaded configuration for {len(settings.tenants)} tenants[/blue]")
		
		tenant_names = None
		if tenants:
			tenant_names = [name.strip() for name in tenants.split(",")]
			console.print(f"[blue]Uploading to selected tenants: {', '.join(tenant_names)}[/blue]")
		
		rows = load_csv_rows(file, mode=mode)
		uploader = MultiTenantUploader(settings, max_workers=max_workers)
		
		console.print(f"[blue]Uploading {len(rows)} rows to tenants...[/blue]")
		result = uploader.upload_all(rows, mode, batch_size, tenant_names, validate_first=not skip_validation)
		
		uploader.print_summary(result)
		
		# Generate reports
		payload = {
			"timestamp": datetime.now().isoformat(),
			"action": "upload-multi",
			"file": str(file),
			"mode": mode.value,
			"batch_size": batch_size,
			"total_rows": len(rows),
			"tenant_results": [
				{
					"tenant_name": r.tenant_name,
					"success": r.success,
					"total_rows": r.total_rows,
					"succeeded": r.succeeded,
					"failed": r.failed,
					"errors": r.errors,
					"validation_errors": r.validation_errors,
					"error_message": r.error_message
				}
				for r in result.tenant_results
			],
			"summary": {
				"total_tenants": result.total_tenants,
				"successful_tenants": result.successful_tenants,
				"failed_tenants": result.failed_tenants,
				"overall_success": result.overall_success,
				"partial_success": result.partial_success
			}
		}
		
		artifacts = emit_multi_tenant_artifact("upload-multi", payload)
		console.print(f"[blue]Reports saved: {len(artifacts)} files[/blue]")
		
		# Write error CSV files for tenants with errors
		if not result.overall_success:
			error_paths = write_multi_tenant_errors_csv(payload["tenant_results"])
			if error_paths:
				console.print(f"[yellow]Error CSV files: {len(error_paths)} files[/yellow]")
		
		if not result.overall_success:
			raise typer.Exit(code=2)
			
	except Exception as e:
		console.print(f"[red]❌ Multi-tenant upload failed: {e}[/red]")
		raise typer.Exit(code=1)


@app.command()
def test_auth_multi(
	config_file: Optional[Path] = typer.Option(None, help="Path to tenant configuration JSON file"),
	tenants: Optional[str] = typer.Option(None, help="Comma-separated list of tenant names to test"),
	max_workers: int = typer.Option(5, min=1, max=20, help="Maximum concurrent authentication tests"),
):
	"""Test authentication for multiple XDR tenants."""
	try:
		settings = get_multi_tenant_settings(config_file)
		console.print(f"[blue]Loaded configuration for {len(settings.tenants)} tenants[/blue]")
		
		tenant_names = None
		if tenants:
			tenant_names = [name.strip() for name in tenants.split(",")]
			console.print(f"[blue]Testing selected tenants: {', '.join(tenant_names)}[/blue]")
		
		uploader = MultiTenantUploader(settings, max_workers=max_workers)
		
		console.print("[blue]Testing authentication for tenants...[/blue]")
		result = uploader.test_auth_all(tenant_names)
		
		uploader.print_summary(result)
		
		# Generate reports
		payload = {
			"timestamp": datetime.now().isoformat(),
			"action": "test-auth-multi", 
			"tenant_results": [
				{
					"tenant_name": r.tenant_name,
					"success": r.success,
					"error_message": r.error_message
				}
				for r in result.tenant_results
			],
			"summary": {
				"total_tenants": result.total_tenants,
				"successful_tenants": result.successful_tenants,
				"failed_tenants": result.failed_tenants,
				"overall_success": result.overall_success
			}
		}
		
		artifacts = emit_multi_tenant_artifact("test-auth-multi", payload)
		console.print(f"[blue]Reports saved: {len(artifacts)} files[/blue]")
		
		if result.overall_success:
			console.print("[green]✅ All tenants authenticated successfully![/green]")
		elif result.partial_success:
			console.print("[yellow]⚠️  Some tenants failed authentication[/yellow]")
		else:
			console.print("[red]❌ All tenants failed authentication[/red]")
			raise typer.Exit(code=1)
			
	except Exception as e:
		console.print(f"[red]❌ Multi-tenant authentication test failed: {e}[/red]")
		raise typer.Exit(code=1)


@app.command()
def list_tenants(
	config_file: Optional[Path] = typer.Option(None, help="Path to tenant configuration JSON file"),
	format: str = typer.Option("table", help="Output format: table or json"),
):
	"""List all configured XDR tenants (without exposing credentials)."""
	try:
		settings = get_multi_tenant_settings(config_file)
		
		# Determine config source
		config_source = "environment (.env file)"
		if config_file:
			config_source = str(config_file)
		
		if format.lower() == "json":
			# JSON output for scripting
			tenant_data = []
			for tenant in settings.tenants:
				tenant_data.append({
					"name": tenant.name,
					"fqdn": tenant.fqdn,
					"api_key_id": tenant.api_key_id,
					"advanced": tenant.advanced,
					"config_source": "json_file" if config_file else "environment"
				})
			
			output = {
				"tenants": tenant_data,
				"total_tenants": len(settings.tenants),
				"config_source": config_source
			}
			
			console.print(json.dumps(output, indent=2))
		
		else:
			# Table output (default)
			console.print(f"[blue]Found {len(settings.tenants)} configured tenants[/blue]")
			console.print(f"[dim]Configuration source: {config_source}[/dim]")
			console.print()
			
			if not settings.tenants:
				console.print("[yellow]⚠️  No tenants configured[/yellow]")
				console.print("[dim]Configure tenants using TENANT{N}_* environment variables in .env file[/dim]")
				console.print("[dim]Or use --config-file to specify a tenants.json file[/dim]")
				return
			
			# Create table
			table = Table(show_header=True, header_style="bold magenta")
			table.add_column("Name", style="cyan")
			table.add_column("FQDN", style="green")
			table.add_column("API Key ID", style="yellow")
			table.add_column("Auth Type", style="blue")
			table.add_column("Status", style="white")
			
			for tenant in settings.tenants:
				# Validate tenant configuration
				status = "[green]✓ Complete[/green]"
				if not tenant.fqdn or not tenant.api_key_id:
					status = "[red]✗ Missing fields[/red]"
				elif not tenant.name:
					status = "[yellow]⚠ No name[/yellow]"
				
				auth_type = "Advanced" if tenant.advanced else "Standard"
				
				table.add_row(
					tenant.name,
					tenant.fqdn,
					tenant.api_key_id,
					auth_type,
					status
				)
			
			console.print(table)
			console.print()
			
			# Summary
			complete_tenants = sum(1 for t in settings.tenants 
								 if t.fqdn and t.api_key_id and t.name)
			if complete_tenants == len(settings.tenants):
				console.print(f"[green]✅ All {len(settings.tenants)} tenants are properly configured[/green]")
			else:
				console.print(f"[yellow]⚠️  {complete_tenants}/{len(settings.tenants)} tenants are properly configured[/yellow]")
			
			console.print("[dim]Use --format json for machine-readable output[/dim]")
			console.print("[dim]API keys are never displayed for security[/dim]")
			
	except Exception as e:
		console.print(f"[red]❌ Failed to load tenant configuration: {e}[/red]")
		console.print("[yellow]💡 Make sure you have either:[/yellow]")
		console.print("   - TENANT1_XDR_FQDN, TENANT1_XDR_API_KEY_ID, etc. in .env file (default)")
		console.print("   - Use --config-file to specify a JSON configuration file")
		raise typer.Exit(code=1)


if __name__ == "__main__":
	app()

