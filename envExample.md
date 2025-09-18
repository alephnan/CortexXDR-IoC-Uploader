### Environment configuration

Use these variables to configure access to your Cortex XDR tenant. Create a `.env` file in the project root (`CargaIOCs/`) with the values below, or set them in your shell environment.

- **XDR_FQDN**: Your tenant FQDN, without protocol. Example: `your-tenant.xdr.us.paloaltonetworks.com`
- **XDR_API_KEY_ID**: API Key ID associated with your key
- **XDR_API_KEY**: API Key (secret)
- **XDR_ADVANCED**: Optional. `true` to use advanced auth headers (default); else `false` for standard auth
- **LOG_LEVEL**: Optional. One of `DEBUG, INFO, WARNING, ERROR` (default `INFO`)

#### Example of single tenant .env file
```env
XDR_FQDN=your-tenant.xdr.us.paloaltonetworks.com
XDR_API_KEY_ID=1234567890
XDR_API_KEY=your_api_key_here
XDR_ADVANCED=true
LOG_LEVEL=INFO
```

#### Example of multi tenant .env file
```
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

Place this `.env` file in the same directory as `pyproject.toml` (the project root: `CargaIOCs/`). The CLI automatically loads it via `python-dotenv`.

#### Set variables in Windows PowerShell (alternative to .env)
```powershell
$env:XDR_FQDN = "your-tenant.xdr.us.paloaltonetworks.com"
$env:XDR_API_KEY_ID = "1234567890"
$env:XDR_API_KEY = "your_api_key_here"
$env:XDR_ADVANCED = "true"
$env:LOG_LEVEL = "INFO"
```

These will apply to the current PowerShell session. Run the CLI in the same session.

#### Security notes
- Do not commit real keys to version control.
- Prefer Advanced keys in production (set `XDR_ADVANCED=true`).
- Rotate keys regularly and scope permissions to least privilege.

