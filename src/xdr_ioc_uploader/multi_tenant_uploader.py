from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.table import Table

from .api_client import XdrApiClient
from .config import Settings
from .models import IndicatorRow
from .multi_tenant_config import TenantConfig, MultiTenantSettings
from .transformers import build_csv_request_data, build_json_objects
from .uploader import Uploader, UploadMode


@dataclass
class TenantUploadResult:
    """Result of upload operation for a single tenant."""
    tenant_name: str
    success: bool
    total_rows: int
    succeeded: int = 0
    failed: int = 0
    errors: List[Dict[str, Any]] = None
    validation_errors: List[Dict[str, Any]] = None
    error_message: Optional[str] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []
        if self.validation_errors is None:
            self.validation_errors = []


@dataclass
class MultiTenantUploadResult:
    """Consolidated result of multi-tenant upload operation."""
    tenant_results: List[TenantUploadResult]
    total_tenants: int
    successful_tenants: int
    failed_tenants: int
    total_rows: int
    
    @property
    def overall_success(self) -> bool:
        """True if all tenants succeeded."""
        return self.failed_tenants == 0
    
    @property
    def partial_success(self) -> bool:
        """True if some tenants succeeded."""
        return self.successful_tenants > 0 and self.failed_tenants > 0


class MultiTenantUploader:
    """Handles IOC uploads to multiple XDR tenants."""
    
    def __init__(self, settings: MultiTenantSettings, max_workers: int = 5):
        self.settings = settings
        self.max_workers = max_workers
        self.console = Console()
        
        # Create uploaders for each tenant
        self.uploaders: Dict[str, Uploader] = {}
        for tenant in settings.tenants:
            # Convert TenantConfig to Settings for compatibility
            tenant_settings = Settings(
                fqdn=tenant.fqdn,
                api_key_id=tenant.api_key_id,
                api_key=tenant.api_key,
                advanced=tenant.advanced,
                log_level=settings.log_level
            )
            self.uploaders[tenant.name] = Uploader(tenant_settings)
    
    def _validate_all(
        self,
        rows: List[IndicatorRow],
        mode: UploadMode,
        tenants: List[TenantConfig],
    ) -> MultiTenantUploadResult:
        """Validate IOC data against all specified tenants."""
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=self.console
        ) as progress:
            task = progress.add_task("Validating against tenants...", total=len(tenants))
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_tenant = {
                    executor.submit(self._validate_tenant, tenant, rows, mode): tenant
                    for tenant in tenants
                }
                
                results = []
                for future in as_completed(future_to_tenant):
                    tenant = future_to_tenant[future]
                    result = future.result()
                    results.append(result)
                    progress.advance(task)
        
        return self._build_multi_tenant_result(results, len(rows))
    
    def upload_all(
        self,
        rows: List[IndicatorRow],
        mode: UploadMode,
        batch_size: int = 1000,
        tenant_names: Optional[List[str]] = None,
    ) -> MultiTenantUploadResult:
        """Upload IOC data to all specified tenants."""
        tenants = self.settings.get_tenants(tenant_names)

        self.console.print("[blue]Validating against all tenants before upload...[/blue]")
        validation_result = self._validate_all(rows, mode, tenants)

        if not validation_result.overall_success:
            self.console.print("[red]Validation failed for some tenants. Upload aborted.[/red]")
            return validation_result

        self.console.print("[green]✓ Validation passed for all tenants[/green]")
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=self.console
        ) as progress:
            task = progress.add_task("Uploading to tenants...", total=len(tenants))
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_tenant = {
                    executor.submit(self._upload_tenant, tenant, rows, mode, batch_size): tenant
                    for tenant in tenants
                }
                
                results = []
                for future in as_completed(future_to_tenant):
                    tenant = future_to_tenant[future]
                    result = future.result()
                    results.append(result)
                    progress.advance(task)
                    
                    # Show per-tenant completion
                    status = "✓" if result.success else "✗"
                    self.console.print(f"{status} {tenant.name}: {result.succeeded}/{result.total_rows}")
        
        return self._build_multi_tenant_result(results, len(rows))
    
    def test_auth_all(self, tenant_names: Optional[List[str]] = None) -> MultiTenantUploadResult:
        """Test authentication for all specified tenants."""
        tenants = self.settings.get_tenants(tenant_names)
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=self.console
        ) as progress:
            task = progress.add_task("Testing authentication...", total=len(tenants))
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_tenant = {
                    executor.submit(self._test_auth_tenant, tenant): tenant
                    for tenant in tenants
                }
                
                results = []
                for future in as_completed(future_to_tenant):
                    tenant = future_to_tenant[future]
                    result = future.result()
                    results.append(result)
                    progress.advance(task)
        
        return self._build_multi_tenant_result(results, 0)
    
    def _validate_tenant(self, tenant: TenantConfig, rows: List[IndicatorRow], 
                        mode: UploadMode) -> TenantUploadResult:
        """Validate IOC data against a single tenant."""
        try:
            uploader = self.uploaders[tenant.name]
            
            if mode == UploadMode.csv:
                request_data = build_csv_request_data(rows)
                validation = uploader.validate_csv(request_data)
            else:
                objects = build_json_objects(rows)
                validation = uploader.validate_json(objects)
            
            errors = validation.get("errors") or validation.get("validation_errors") or []
            success = len(errors) == 0
            
            return TenantUploadResult(
                tenant_name=tenant.name,
                success=success,
                total_rows=len(rows),
                validation_errors=errors
            )
            
        except Exception as e:
            return TenantUploadResult(
                tenant_name=tenant.name,
                success=False,
                total_rows=len(rows),
                error_message=str(e)
            )
    
    def _upload_tenant(self, tenant: TenantConfig, rows: List[IndicatorRow],
                      mode: UploadMode, batch_size: int) -> TenantUploadResult:
        """Upload IOC data to a single tenant."""
        try:
            uploader = self.uploaders[tenant.name]
            
            if mode == UploadMode.csv:
                reply = uploader.commit_csv(rows, batch_size=batch_size)
            else:
                reply = uploader.commit_json(rows, batch_size=batch_size)
            
            succeeded = reply.get("succeeded", 0)
            failed = reply.get("failed", 0)
            errors = reply.get("errors") or reply.get("failed_rows") or []
            
            return TenantUploadResult(
                tenant_name=tenant.name,
                success=failed == 0,
                total_rows=len(rows),
                succeeded=succeeded,
                failed=failed,
                errors=errors
            )
            
        except Exception as e:
            return TenantUploadResult(
                tenant_name=tenant.name,
                success=False,
                total_rows=len(rows),
                error_message=str(e)
            )
    
    def _test_auth_tenant(self, tenant: TenantConfig) -> TenantUploadResult:
        """Test authentication for a single tenant."""
        try:
            # Create temporary settings and client for testing
            tenant_settings = Settings(
                fqdn=tenant.fqdn,
                api_key_id=tenant.api_key_id,
                api_key=tenant.api_key,
                advanced=tenant.advanced
            )
            client = XdrApiClient(tenant_settings)
            response = client.test_authentication()
            
            return TenantUploadResult(
                tenant_name=tenant.name,
                success=True,
                total_rows=0
            )
            
        except Exception as e:
            return TenantUploadResult(
                tenant_name=tenant.name,
                success=False,
                total_rows=0,
                error_message=str(e)
            )
    
    def _build_multi_tenant_result(self, results: List[TenantUploadResult], 
                                  total_rows: int) -> MultiTenantUploadResult:
        """Build consolidated multi-tenant result."""
        successful_tenants = sum(1 for r in results if r.success)
        failed_tenants = len(results) - successful_tenants
        
        return MultiTenantUploadResult(
            tenant_results=results,
            total_tenants=len(results),
            successful_tenants=successful_tenants,
            failed_tenants=failed_tenants,
            total_rows=total_rows
        )
    
    def print_summary(self, result: MultiTenantUploadResult) -> None:
        """Print summary table of multi-tenant operation results."""
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Tenant")
        table.add_column("Status")
        table.add_column("Succeeded")
        table.add_column("Failed") 
        table.add_column("Errors")
        
        for tenant_result in result.tenant_results:
            status = "[green]✓[/green]" if tenant_result.success else "[red]✗[/red]"
            errors = len(tenant_result.errors) + len(tenant_result.validation_errors)
            error_msg = tenant_result.error_message if tenant_result.error_message else ""
            
            table.add_row(
                tenant_result.tenant_name,
                status,
                str(tenant_result.succeeded),
                str(tenant_result.failed),
                f"{errors}" + (f" ({error_msg[:50]}...)" if error_msg else "")
            )
        
        self.console.print(table)
        
        # Overall summary
        if result.overall_success:
            self.console.print(f"[green]✅ All {result.total_tenants} tenants succeeded[/green]")
        elif result.partial_success:
            self.console.print(f"[yellow]⚠️  {result.successful_tenants}/{result.total_tenants} tenants succeeded[/yellow]")
        else:
            self.console.print(f"[red]❌ All {result.total_tenants} tenants failed[/red]")
