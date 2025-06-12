import os
import requests
import pandas as pd
from deltalake.writer import write_deltalake
from typing import Dict, Any, List

# Import Azure SDK libraries
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential, CredentialUnavailableError

# --- 1. CONFIGURATION ---

# The URL of your Azure Key Vault, set as an environment variable
KEY_VAULT_URL = os.getenv("KEY_VAULT_URL")

# The names of the secrets as stored in Azure Key Vault
SECRET_NAMES = {
    "api_url": "apiUrl",
    "storage_account_name": "storageAccountName",
    "storage_account_key": "storageAccountKey",
    "storage_container_name": "storageContainerName",
    "delta_table_path": "deltaTablePath"
}


def get_secrets_from_keyvault(vault_url: str) -> Dict[str, str]:
    """
    Fetches secrets from Azure Key Vault using DefaultAzureCredential.

    Args:
        vault_url: The URL of the Azure Key Vault.

    Returns:
        A dictionary containing the secret values.
    """
    print(f"Fetching secrets from Azure Key Vault: {vault_url}")
    secrets = {}
    try:
        # DefaultAzureCredential will automatically use the best auth method
        # (e.g., az login, Managed Identity, Service Principal)
        credential = DefaultAzureCredential()
        client = SecretClient(vault_url=vault_url, credential=credential)

        for key, secret_name in SECRET_NAMES.items():
            print(f"  - Retrieving secret: {secret_name}")
            retrieved_secret = client.get_secret(secret_name)
            secrets[key] = retrieved_secret.value
        
        print("All secrets retrieved successfully.")
        return secrets
    except CredentialUnavailableError:
        print("Error: Azure credential not available.")
        print("Please log in via 'az login' or ensure Managed Identity is configured.")
        raise
    except Exception as e:
        print(f"An error occurred while fetching secrets: {e}")
        raise

def fetch_data_from_api(url: str) -> List[Dict[str, Any]]:
    """Fetches data from a REST API endpoint."""
    print(f"Fetching data from API...")
    try:
        response = requests.get(url)
        response.raise_for_status()
        print("Data fetched successfully.")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
        raise

def process_data_to_dataframe(data: List[Dict[str, Any]]) -> pd.DataFrame:
    """Processes raw JSON data and converts it into a pandas DataFrame."""
    print("Processing data into a pandas DataFrame...")
    if not data:
        print("Warning: API returned no data.")
        return pd.DataFrame()
    df = pd.json_normalize(data)
    print(f"DataFrame created with {len(df)} rows and {len(df.columns)} columns.")
    return df

def write_df_to_adls_delta(df: pd.DataFrame, config: Dict[str, str], mode: str = "overwrite"):
    """Writes a pandas DataFrame to ADLS Gen2 as a Delta table."""
    if df.empty:
        print("DataFrame is empty. Skipping write operation.")
        return

    full_table_path = (
        f"abfss://{config['storage_container_name']}@"
        f"{config['storage_account_name']}.dfs.core.windows.net/"
        f"{config['delta_table_path']}"
    )
    
    storage_options = {
        "azure_storage_account_name": config['storage_account_name'],
        "azure_storage_account_key": config['storage_account_key']
    }
    
    print(f"Writing DataFrame to Delta table at: {full_table_path} (mode: {mode})")
    try:
        write_deltalake(
            table_or_uri=full_table_path,
            data=df,
            mode=mode,
            schema_mode="overwrite" if mode == "overwrite" else "merge",
            storage_options=storage_options
        )
        print("Successfully wrote data to Delta Lake in ADLS Gen2.")
    except Exception as e:
        print(f"Error writing to Delta Lake: {e}")
        raise

# --- 2. MAIN EXECUTION ---
if __name__ == "__main__":
    print("--- Starting Secure API to ADLS Delta Lake Ingestion ---")
    
    if not KEY_VAULT_URL:
        print("Error: KEY_VAULT_URL environment variable is not set.")
        exit(1)
        
    try:
        # Step 1: Securely fetch all configuration from Azure Key Vault
        config = get_secrets_from_keyvault(KEY_VAULT_URL)
        
        # Step 2: Fetch data from the API
        api_data = fetch_data_from_api(config['api_url'])
        
        # Step 3: Process data into a DataFrame
        dataframe = process_data_to_dataframe(api_data)
        
        # Step 4: Write the DataFrame to ADLS as a Delta table
        write_df_to_adls_delta(df=dataframe, config=config, mode="overwrite")
        
        print("--- Ingestion process completed successfully! ---")
        
    except Exception as e:
        print(f"--- Ingestion process failed: {e} ---")
        exit(1)