import boto3
from datetime import datetime

def get_dates(force_date=None):
    """
    Get date information in various formats.
    
    Args:
        force_date (str, optional): Force a specific date in 'YYYY-MM-DD' format
    
    Returns:
        dict: Dictionary containing year, month, day, short_date, and formatted_date
    """
    if force_date:
        current_date = datetime.strptime(force_date, '%Y-%m-%d')
    else:
        current_date = datetime.now()

    return {
        'year': current_date.strftime('%Y'),
        'month': current_date.strftime('%m'),
        'day': current_date.strftime('%d'),
        'short_date': current_date.strftime('%Y%m%d'),
        'formatted_date': current_date.strftime('%Y-%m-%d')
    }

def get_multiple_parameters(parameter_names, ssm_client=None):
    """
    Retrieve multiple parameters from AWS Systems Manager Parameter Store.
    
    Args:
        parameter_names (list): List of parameter names to retrieve
        ssm_client (boto3.client, optional): SSM client. If None, creates a new one.
    
    Returns:
        dict: Dictionary mapping parameter names to their values
    
    Raises:
        Exception: If parameters are missing or error occurs
    """
    if ssm_client is None:
        ssm_client = boto3.client('ssm')

    try:
        response = ssm_client.get_parameters(
            Names=parameter_names,
            WithDecryption=True
        )

        # Check for missing parameters
        if response.get('InvalidParameters'):
            raise Exception(f"Missing parameters: {response['InvalidParameters']}")

        params = {}
        for param in response['Parameters']:
            params[param['Name']] = param['Value']
        
        return params
    except Exception as e:
        print(f"Error getting parameters: {e}")
        raise