import boto3
from botocore.exceptions import ClientError


def get_secret(secret_name: str = "partner_code", region_name: str = "eu-west-2"):
    """
    A provided AWS function from the [AWS Secrets Manager](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/secrets-manager.html) python documentation for retrieving an AWS secret value.
    Leverages the [get_secret_value](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/secretsmanager/client/get_secret_value.html) request and response syntax.

    Secrets Manager decrypts the secret value using the associated KMS CMK.
    Depending on whether the secret was a string or binary, only one of the return fields will be populated.
    It is also worth noting that secrets created via the Secrets Manager console will always be a SecretString.

    Parameters:
        secret_name (str): The name of the secret as stored in AWS Secrets Manager. Defaults to be the current sole use case.
        region_name (str): The name of the region the secret is stored in. Defaults to eu-west-2 - London.

    Returns:
        text_secret_data (str): The string version of the secret value (if applicable)
        binary_secret_data (Base64-encoded binary data object): The binary version of the secret value (if applicable)
    """

    session = boto3.session.Session()
    client = session.client(
        service_name="secretsmanager",
        region_name=region_name,
    )

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            raise Exception("The requested secret " + secret_name + " was not found")
        elif e.response["Error"]["Code"] == "InvalidRequestException":
            print("The request was invalid due to:", e)
        elif e.response["Error"]["Code"] == "InvalidParameterException":
            print("The request had invalid params:", e)
        elif e.response["Error"]["Code"] == "DecryptionFailure":
            print(
                "The requested secret can't be decrypted using the provided KMS key:", e
            )
        elif e.response["Error"]["Code"] == "InternalServiceError":
            print("An error occurred on service side:", e)
    else:
        if "SecretString" in get_secret_value_response:
            text_secret_data = get_secret_value_response["SecretString"]
            return text_secret_data
        else:
            binary_secret_data = get_secret_value_response["SecretBinary"]
            return binary_secret_data
