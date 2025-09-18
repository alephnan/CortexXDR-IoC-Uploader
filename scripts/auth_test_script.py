#!/usr/bin/env python3
"""
Standalone XDR API Authentication Test Script

This script tests authentication with your Cortex XDR tenant.
It can be run independently of the main CLI application.

Usage:
    python scripts/test_auth.py
    
Or with custom credentials:
    XDR_FQDN=your-tenant.xdr.us.paloaltonetworks.com \
    XDR_API_KEY_ID=123 \
    XDR_API_KEY=your_key \
    XDR_ADVANCED=true \
    python scripts/test_auth.py
"""

import sys
import os
from pathlib import Path

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from xdr_ioc_uploader.config import get_settings
from xdr_ioc_uploader.api_client import XdrApiClient
import requests


def test_basic_connectivity(fqdn: str) -> bool:
    """Test basic network connectivity to the XDR tenant."""
    try:
        url = f"https://{fqdn}"
        response = requests.get(url, timeout=10)
        print(f"✅ Network connectivity to {fqdn}: OK (HTTP {response.status_code})")
        return True
    except requests.exceptions.RequestException as e:
        print(f"❌ Network connectivity to {fqdn}: FAILED ({e})")
        return False


def test_authentication_headers(client: XdrApiClient) -> None:
    """Test and display authentication headers."""
    print("\n📋 Authentication Headers:")
    print("-" * 40)
    
    headers = client._headers()
    for key, value in headers.items():
        if key == "Authorization":
            # Mask the actual key/signature for security
            if len(value) == 64:  # SHA256 hash
                display_value = f"{value[:8]}...{value[-8:]} (SHA256 signature)"
            else:
                display_value = f"{value[:8]}...{value[-4:]} (API key)"
        else:
            display_value = value
        print(f"  {key}: {display_value}")


def test_api_authentication(client: XdrApiClient) -> tuple[bool, dict]:
    """Test actual API authentication."""
    try:
        print("\n🔐 Testing API Authentication...")
        response = client.test_authentication()
        print("✅ API Authentication: SUCCESS")
        return True, response
    except requests.exceptions.HTTPError as e:
        if "401" in str(e):
            print("❌ API Authentication: FAILED (401 Unauthorized)")
            print("   Possible issues:")
            print("   - Incorrect API Key ID")
            print("   - Incorrect API Key")
            print("   - Wrong advanced auth setting")
            print("   - API key expired or disabled")
        elif "403" in str(e):
            print("❌ API Authentication: FAILED (403 Forbidden)")
            print("   Possible issues:")
            print("   - API key doesn't have required permissions")
            print("   - Account limitations")
        else:
            print(f"❌ API Authentication: FAILED ({e})")
        return False, {}
    except Exception as e:
        print(f"❌ API Authentication: FAILED ({e})")
        return False, {}


def analyze_response(response: dict) -> None:
    """Analyze and display the API response."""
    print("\n📊 API Response Analysis:")
    print("-" * 40)
    
    for key, value in response.items():
        if key == "validation_errors":
            if isinstance(value, list) and value:
                print(f"  {key}: {len(value)} validation errors")
                for i, error in enumerate(value[:3], 1):  # Show first 3 errors
                    print(f"    {i}. {error}")
                if len(value) > 3:
                    print(f"    ... and {len(value) - 3} more errors")
            else:
                print(f"  {key}: No validation errors")
        elif key == "request_data":
            # Don't display the full CSV data
            print(f"  {key}: [CSV data - {len(str(value))} characters]")
        else:
            print(f"  {key}: {value}")


def main():
    """Main test function."""
    print("🚀 XDR API Authentication Test")
    print("=" * 50)
    
    # 1. Load configuration
    try:
        settings = get_settings()
        print(f"📍 Target: {settings.fqdn}")
        print(f"🔑 API Key ID: {settings.api_key_id}")
        print(f"🔒 Advanced Auth: {settings.advanced}")
        print(f"📝 Log Level: {settings.log_level}")
    except Exception as e:
        print(f"❌ Configuration Error: {e}")
        print("\nMake sure you have a .env file with:")
        print("  XDR_FQDN=your-tenant.xdr.us.paloaltonetworks.com")
        print("  XDR_API_KEY_ID=your_key_id")
        print("  XDR_API_KEY=your_api_key")
        print("  XDR_ADVANCED=true/false")
        return 1
    
    # 2. Test basic connectivity
    if not test_basic_connectivity(settings.fqdn):
        return 1
    
    # 3. Initialize API client
    client = XdrApiClient(settings)
    
    # 4. Show authentication headers
    test_authentication_headers(client)
    
    # 5. Test actual authentication
    success, response = test_api_authentication(client)
    
    if success:
        analyze_response(response)
        print("\n🎉 All tests passed! Your XDR API authentication is working correctly.")
        return 0
    else:
        print("\n💔 Authentication test failed. Please check your credentials.")
        return 1


if __name__ == "__main__":
    exit(main())