## XDR IOC Uploader

CLI to validate and upload IOC CSV/JSON files to Cortex XDR. Supports both single and multi-tenant operations with concurrent processing, local validation, rate limiting, retries, chunked uploads, and rich reporting.

### Features
- **Multi-tenant support**: Upload IOCs to multiple XDR tenants simultaneously
- **Concurrent operations**: Parallel uploads with configurable worker limits
- Validate IOC files (CSV/JSON) locally for structure and format
- Advanced authentication by default (nonce + timestamp + SHA256 signature)
- Local validation for schema and format checks (no network calls)
- Chunked uploads with batch sizing
- **Per-tenant reporting**: Individual and consolidated reports
- Automatic report artifacts under `reports/`
- Local CSV preprocessing commands for classification and bulk metadata adjustments

---

## Requirements
- Python 3.9+
- Access to a Cortex XDR tenant and API key (Advanced auth recommended)

---

## Installation
```bash
pip install -e .          # install locally in editable mode
# with dev tools (pytest, ruff, etc.)
pip install -e .[dev]
```

---

## Environment Setup

### Single Tenant Configuration
For single tenant operations, the CLI reads configuration from environment variables and a `.env` file in the project root.

Required variables:
- `XDR_FQDN` (without protocol, e.g., `api-yourtenant.xdr.us.paloaltonetworks.com`)
- `XDR_API_KEY_ID`
- `XDR_API_KEY`

Optional:
- `XDR_ADVANCED` (default: `true`) – set to `false` to use standard auth
- `LOG_LEVEL` (default: `INFO`)

Create `.env` (recommended):
```env
XDR_FQDN=your-tenant.xdr.us.paloaltonetworks.com
XDR_API_KEY_ID=1234567890
XDR_API_KEY=your_api_key_here
XDR_ADVANCED=true #Type of API key
LOG_LEVEL=INFO
```

### Multi-Tenant Configuration

**Default**: The tool uses `.env` file configurations by default. JSON files are only used when explicitly specified with `--config-file`.

#### Option 1: Environment Variables (.env file) - Default Method
Add numbered tenant configurations to your `.env` file:
```env
# First tenant
TENANT1_XDR_FQDN=prod-tenant.xdr.us.paloaltonetworks.com
TENANT1_XDR_API_KEY_ID=1234567890
TENANT1_XDR_API_KEY=your_production_api_key
TENANT1_XDR_NAME=production
TENANT1_XDR_ADVANCED=true

# Second tenant
TENANT2_XDR_FQDN=staging-tenant.xdr.us.paloaltonetworks.com
TENANT2_XDR_API_KEY_ID=0987654321
TENANT2_XDR_API_KEY=your_staging_api_key
TENANT2_XDR_NAME=staging
TENANT2_XDR_ADVANCED=true

# Third tenant ... 

# Global settings
LOG_LEVEL=INFO
```

#### Option 2: JSON Configuration File (When Explicitly Specified)
Create `tenants.json` in the project root and use `--config-file tenants.json`:
```json
{
  "tenants": [
    {
      "name": "production",
      "fqdn": "prod-tenant.xdr.us.paloaltonetworks.com",
      "api_key_id": "1234567890",
      "api_key": "your_production_api_key",
      "advanced": true
    },
    {
      "name": "staging",
      "fqdn": "staging-tenant.xdr.us.paloaltonetworks.com",
      "api_key_id": "0987654321",
      "api_key": "your_staging_api_key",
      "advanced": true
    }
  ],
  "log_level": "INFO"
}
```

**Note**: JSON files are only used when you specify `--config-file`. The `.env` file is always the default.

Windows PowerShell (alternative to .env):
```powershell
$env:XDR_FQDN = "your-tenant.xdr.us.paloaltonetworks.com"
$env:XDR_API_KEY_ID = "1234567890"
$env:XDR_API_KEY = "your_api_key_here"
$env:XDR_ADVANCED = "true"
$env:LOG_LEVEL = "INFO"
```

Note: Explicit environment variables in the shell take precedence over `.env`.

---

## Quickstart

### Single Tenant Operations
```bash
# 1) Validate a CSV file locally 
xdr-ioc-uploader validate examples/csv_template.csv

# 2) Validate and upload to Cortex XDR (requires credentials)
xdr-ioc-uploader upload examples/csv_template.csv

# 3) Commit upload in batches (example: 500 rows per request)
xdr-ioc-uploader upload examples/csv_template.csv --batch-size 500

# 4) Test authentication quickly (Uploads 127.0.0.1 as an IOC)
xdr-ioc-uploader test-auth
```

### Multi-Tenant Operations
```bash
# 1) List all configured tenants
xdr-ioc-uploader list-tenants

# 2) Test authentication for all configured tenants
xdr-ioc-uploader test-auth-multi

# 3) Validate CSV against all tenants
xdr-ioc-uploader validate-multi examples/csv_template.csv

# 4) Upload to all configured tenants
xdr-ioc-uploader upload-multi examples/csv_template.csv

# 5) Upload to specific tenants only
xdr-ioc-uploader upload-multi examples/csv_template.csv --tenants "production,staging"

# 6) Use custom config file and limit concurrent workers
xdr-ioc-uploader upload-multi examples/csv_template.csv --config-file tenants.json --max-workers 3

# 7) Skip validation phase (not recommended)
xdr-ioc-uploader upload-multi examples/csv_template.csv --skip-validation
```

Artifacts (JSON reports) are written under `reports/` with timestamped filenames. Multi-tenant operations generate both consolidated reports and per-tenant reports.

---

## CLI Reference

### Single Tenant Commands

#### validate
```bash
xdr-ioc-uploader validate <file> [--mode csv|json]
```
- Validates the file structure and format locally (no network calls).
- Summary and detailed replies are persisted to `reports/`.

#### upload
```bash
xdr-ioc-uploader upload <file> [--mode csv|json] [--batch-size N]
```
- Validates the entire file first. If clean, uploads in batches.

#### test-auth
```bash
xdr-ioc-uploader test-auth
```
- Sends a minimal validation request to verify your credentials and connectivity.

### Multi-Tenant Commands

#### list-tenants
```bash
xdr-ioc-uploader list-tenants [OPTIONS]
```
Options:
- `--config-file PATH` - Path to tenant configuration JSON file
- `--format table|json` - Output format (default: table)

List all configured tenants with their configuration status. Never exposes API keys or credentials for security.

#### validate-multi
```bash
xdr-ioc-uploader validate-multi <file> [OPTIONS]
```
Options:
- `--mode csv|json` - Validation mode (default: csv)
- `--config-file PATH` - Path to tenant configuration JSON file
- `--tenants "name1,name2"` - Comma-separated list of tenant names to validate against
- `--max-workers N` - Maximum concurrent tenant validations (default: 5)

#### upload-multi
```bash
xdr-ioc-uploader upload-multi <file> [OPTIONS]
```
Options:
- `--mode csv|json` - Upload mode (default: csv)
- `--batch-size N` - Rows per request for commit phase (default: 1000)
- `--config-file PATH` - Path to tenant configuration JSON file
- `--tenants "name1,name2"` - Comma-separated list of tenant names to upload to
- `--max-workers N` - Maximum concurrent tenant uploads (default: 5)
- `--skip-validation` - Skip validation phase (not recommended)

#### test-auth-multi
```bash
xdr-ioc-uploader test-auth-multi [OPTIONS]
```
Options:
- `--config-file PATH` - Path to tenant configuration JSON file
- `--tenants "name1,name2"` - Comma-separated list of tenant names to test
- `--max-workers N` - Maximum concurrent authentication tests (default: 5)

### File Operations Commands

Offline helpers to adjust CSV indicators before validation or upload. All commands share these options:
- `--output/-o PATH` - Write to a new file (default: `<name>-<command>.csv`)
- `--in-place` - Overwrite the source file (creates `.bak` unless `--no-backup`)
- `--no-backup` - Skip backup creation when using `--in-place`
- `--only-empty` - Change only rows where the target column is empty/blank
- `--dry-run` - Show the summary without writing any file
- Per-type overrides: use `--hash`, `--ip`, `--domain`, `--path`, `--filename` to apply the command default to that type, or `--hash-value VALUE` / `--ip-value VALUE` etc. for explicit per-type overrides

#### file-classify
```bash
xdr-ioc-uploader file-classify <file> [OPTIONS]
```
- Infers `type` from the indicator value (hash, IP, domain, path, filename)
- `--force` overwrites existing values, `--only-empty` fills blanks only

#### file-reputation
```bash
xdr-ioc-uploader file-reputation <value> <file> [OPTIONS]
```
- Sets `reputation` (`bad`, `good`, `suspicious`, `unknown`, `no reputation` to clear)
- Per-type flags override the default for matching indicator types (`--hash` applies the command value; `--hash-value VALUE` sets a custom one)

#### file-severity
```bash
xdr-ioc-uploader file-severity <value> <file> [OPTIONS]
```
- Sets `severity` (`high`, `medium`, `low`, `critical`, `informational` -> `INFO`)
- Per-type overrides apply the provided severity to matching types (`--hash` applies the command value; `--hash-value VALUE` sets a custom one)

#### file-comment
```bash
xdr-ioc-uploader file-comment <text> <file> [OPTIONS]
```
- Replaces the `comment` field; quote the text when it contains spaces
- Per-type overrides set custom comments on specific indicator types (`--hash` applies the command value; `--hash-value VALUE` sets a custom one)

#### file-reliability
```bash
xdr-ioc-uploader file-reliability <value> <file> [OPTIONS]
```
- Sets `reliability` (`A`, `B`, `C`, `D`, `E`, `F`, `G`)
- Per-type overrides apply different reliability grades when needed (`--hash` applies the command value; `--hash-value VALUE` sets a custom one)

---

## File Formats

### CSV (preferred)
Columns (header required):
```
indicator,type,severity,reputation,expiration_date,comment,reliability
```

Notes:
- Required: `indicator`, `type`, `severity`
- Optional: `reputation`, `expiration_date`, `comment`, `reliability`
- `type` one of: `HASH, IP, PATH (CSV only), DOMAIN_NAME, FILENAME`
- `severity` one of: `INFO, LOW, MEDIUM, HIGH, CRITICAL`
- `reputation` one of: `GOOD, BAD, SUSPICIOUS, UNKNOWN`
- `reliability` one of: `A, B, C, D, E, F, G`
- `expiration_date`: ISO-8601 (e.g., `2025-12-31T00:00:00Z`), epoch (s/ms), or `Never`
- Empty rows are ignored automatically

See `examples/csv_template.csv` for a sample.

### JSON
- The set of allowed `type` values excludes `PATH` when using JSON objects.

---

## Troubleshooting

### Single Tenant Issues

#### Using the wrong FQDN
Ensure `XDR_FQDN` does not include `https://`. Example: `api-<tenant>.xdr.us.paloaltonetworks.com`.

#### 401 Unauthorized
- Verify `XDR_API_KEY_ID` and `XDR_API_KEY`
- Confirm `XDR_ADVANCED=true` matches your key type (set `false` for standard keys)
- Ensure the API key has the necessary permissions

#### `.env` ignored
Environment variables set in the shell override `.env`. Clear them or open a fresh shell.
The file SHOULD be named JUST `.env` not `keys.env` or `something.env`

### Multi-Tenant Issues

#### Configuration not found
- Use `xdr-ioc-uploader list-tenants` to verify your tenant configuration
- Ensure `tenants.json` exists in project root, or use `--config-file PATH`
- For environment variables, use `TENANT1_*`, `TENANT2_*` format
- Check tenant names match exactly when using `--tenants` option

#### Partial tenant failures
- Multi-tenant operations continue even if some tenants fail
- Check individual tenant reports in `reports/` directory
- Look for tenant-specific error CSV files: `reports/errors-{tenant_name}.csv`

#### Performance tuning
- Adjust `--max-workers` based on your system and network capacity
- Consider `--batch-size` for large files to balance memory and API limits
- Use `--tenants` to target specific tenants during testing

### General Issues

#### CSV validation errors
- Check header matches exactly: `indicator,type,severity,reputation,expiration_date,comment`
- Ensure required fields are not empty
- Empty trailing rows are ignored, but partially empty rows will error with a row number

---

## Development
```bash
pip install -e .[dev]
pytest -v          # run tests
ruff check .       # lint
```

---

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
