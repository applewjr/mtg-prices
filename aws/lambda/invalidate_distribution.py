import boto3

from utils import get_multiple_parameters

ssm = boto3.client('ssm')

def lambda_handler(event, context):
    """
    Invalidates the CloudFront cache for the MTG price CSV after each daily pipeline run.
    Without this, CloudFront serves the previous day's file until the TTL expires (30 min).
    """
    # Get configuration
    param_names = [
        '/mtg/s3/paths/final_output_key',
        '/mtg/cloudfront/distribution'
    ]
    params = get_multiple_parameters(param_names, ssm)
    final_output_key = params['/mtg/s3/paths/final_output_key']
    cloudfront_distribution = params['/mtg/cloudfront/distribution']

    cf = boto3.client('cloudfront')
    cf.create_invalidation(
        DistributionId=cloudfront_distribution,
        InvalidationBatch={
            'Paths': {
                'Quantity': 1,
                'Items': [f'/{final_output_key}']
            },
            'CallerReference': str(event.get('execution_id', 'manual'))
        }
    )
