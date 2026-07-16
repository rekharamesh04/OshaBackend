import json
import os
import logging
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize Cognito client
cognito = boto3.client('cognito-idp')

def build_response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps(body)
    }

def lambda_handler(event, context):
    """
    POST /api/users/invite
    Body: { "email": "user@example.com", "name": "John Doe" }
    """
    logger.info(f"Event: {json.dumps(event)}")
    
    # 1. Get the User Pool ID from environment variables
    user_pool_id = os.environ.get('USER_POOL_ID')
    if not user_pool_id:
        logger.error("USER_POOL_ID environment variable is missing.")
        return build_response(500, {"error": "Server misconfiguration. Missing User Pool ID."})

    # 2. Extract JWT Claims
    claims = event.get("requestContext", {}).get("authorizer", {}).get("claims", {})
    admin_company_key = claims.get("custom:company_key", "")
    
    groups = claims.get("cognito:groups", "")
    if isinstance(groups, str):
        groups = [g.strip() for g in groups.split(",")]
    elif not groups:
        groups = []

    # 3. Parse the requested new user details
    body_str = event.get("body", "{}")
    try:
        body = json.loads(body_str) if isinstance(body_str, str) else body_str
    except Exception as e:
        return build_response(400, {"error": "Invalid JSON body"})

    # 4. SuperAdmin override logic
    if "SuperAdmin" in groups:
        admin_company_key = body.get("company_key", "").strip()
        if not admin_company_key:
            return build_response(400, {"error": "SuperAdmins must provide 'company_key' in the request body."})
    
    # If not a SuperAdmin and still no company_key, unauthorized
    if not admin_company_key:
        logger.warning("No company_key found in authorizer claims. Make sure JWT Authorizer is configured.")
        # Fallback for local testing
        admin_company_key = body.get("company_key", "").strip()
        if not admin_company_key:
            return build_response(403, {"error": "Unauthorized. Could not determine company key."})

    email = body.get("email", "").strip()
    name = body.get("name", "").strip()
    group_name = body.get("group", "User").strip()

    if not email:
        return build_response(400, {"error": "Missing required field: email"})

    try:
        # 5. Create the user in Cognito and automatically assign the company_key
        cognito_kwargs = {
            "UserPoolId": user_pool_id,
            "Username": email,
            "UserAttributes": [
                {
                    'Name': 'email',
                    'Value': email
                },
                {
                    'Name': 'email_verified',
                    'Value': 'true'
                },
                {
                    'Name': 'name',
                    'Value': name if name else email.split('@')[0]
                },
                {
                    'Name': 'custom:company_key',
                    'Value': admin_company_key
                }
            ],
            "DesiredDeliveryMediums": ['EMAIL']
        }
        
        if body.get('suppress_email'):
            cognito_kwargs["MessageAction"] = "SUPPRESS"
            
        response = cognito.admin_create_user(**cognito_kwargs)
        
        logger.info(f"Successfully created user {email} for company {admin_company_key}")
        
        # 5. Add user to the specified Cognito group
        try:
            cognito.admin_add_user_to_group(
                UserPoolId=user_pool_id,
                Username=email,
                GroupName=group_name
            )
            logger.info(f"Successfully added {email} to group {group_name}")
        except Exception as e:
            logger.error(f"Failed to add user to group {group_name}: {e}")
            # We don't fail the whole request, but log the error
        
        return build_response(200, {
            "message": "User invited successfully",
            "email": email,
            "company_key": admin_company_key,
            "group": group_name
        })

    except cognito.exceptions.UsernameExistsException:
        return build_response(409, {"error": "A user with this email already exists."})
    except Exception as e:
        logger.exception("Failed to create user in Cognito")
        return build_response(500, {"error": f"Failed to create user: {str(e)}"})
