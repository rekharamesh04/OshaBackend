import json
import os
import logging
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize Cognito client
cognito = boto3.client('cognito-idp')

# --- Role hierarchy: higher number = more privilege ---
ROLE_HIERARCHY = {
    "SuperAdmin": 4,
    "Admin": 3,
    "Manager": 2,
    "User": 1
}

ALLOWED_GROUPS = set(ROLE_HIERARCHY.keys())


def get_caller_highest_role(groups):
    """Return the highest role level from the caller's group list."""
    if not groups:
        return 0, None
    best_level = 0
    best_role = None
    for g in groups:
        level = ROLE_HIERARCHY.get(g, 0)
        if level > best_level:
            best_level = level
            best_role = g
    return best_level, best_role


def validate_invite_permission(caller_groups, target_group):
    """
    Validate that the caller is allowed to invite a user into target_group.
    Rules:
      - Only Admin and above can invite.
      - You can only invite roles BELOW your own level.
      - SuperAdmin can invite any lower role to any company.
      - Admin  can invite Manager and User  (own company only — enforced elsewhere).
      - Manager can invite User              (own company only — enforced elsewhere).
      - User   CANNOT invite anyone.
    """
    caller_level, caller_role = get_caller_highest_role(caller_groups)

    if caller_level == 0:
        return False, "You do not belong to any recognised role group."

    if caller_level <= ROLE_HIERARCHY.get("User", 1):
        return False, "Users do not have permission to invite others."

    target_level = ROLE_HIERARCHY.get(target_group, 0)
    if target_level == 0:
        return False, f"'{target_group}' is not a valid role."

    if target_level >= caller_level:
        return False, f"A {caller_role} cannot invite a {target_group}. You can only invite roles below your own."

    return True, "OK"

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
    Router for User Management endpoints:
        POST /api/users/invite   → Invite a new user
        GET  /api/users/list     → List users (filtered by company_key)
        GET  /api/users/{email}  → Get a single user's profile
    """
    logger.info(f"Event: {json.dumps(event)}")

    # 1. Get the User Pool ID from environment variables
    user_pool_id = os.environ.get('USER_POOL_ID')
    if not user_pool_id:
        logger.error("USER_POOL_ID environment variable is missing.")
        return build_response(500, {"error": "Server misconfiguration. Missing User Pool ID."})

    # 2. Extract JWT Claims
    authorizer = event.get("requestContext", {}).get("authorizer", {})
    logger.info(f"RAW authorizer: {json.dumps(authorizer, default=str)}")
    claims = (
        authorizer.get("jwt", {}).get("claims")
        or authorizer.get("claims")
        or {}
    )
    caller_company_key = claims.get("custom:company_key", "")

    groups = claims.get("cognito:groups", "")
    if isinstance(groups, str):
        groups = groups.strip()
        if groups.startswith("[") and groups.endswith("]"):
            groups = groups[1:-1]
        groups = [g.strip() for g in groups.split(",") if g.strip()]
    elif not groups:
        groups = []

    # 3. Route based on HTTP method and path
    http_method = event.get("httpMethod", "").upper()
    path = event.get("path", "")

    if http_method == "GET" and path.rstrip("/") == "/api/users/list":
        return handle_list_users(event, user_pool_id, groups, caller_company_key)
    elif http_method == "GET" and path.startswith("/api/users/"):
        return handle_get_user(event, user_pool_id, groups, caller_company_key, path)
    elif http_method == "POST" and path.rstrip("/") == "/api/users/invite":
        return handle_invite_user(event, user_pool_id, groups, caller_company_key)
    elif http_method == "POST" and path.rstrip("/") == "/api/users/resend-invite":
        return handle_resend_invite(event, user_pool_id, groups, caller_company_key)
    elif http_method == "DELETE" and path.startswith("/api/users/"):
        return handle_delete_user(event, user_pool_id, groups, caller_company_key, path, claims)
    else:
        return build_response(404, {"error": f"Route not found: {http_method} {path}"})


# ───────────────────────────────────────────────────────────────
# HANDLER: GET /api/users/list
# ───────────────────────────────────────────────────────────────
def handle_list_users(event, user_pool_id, groups, caller_company_key):
    """
    List all users for a company.
      - SuperAdmin: must pass ?company_key=xxx (can view any company)
      - Admin/Manager: automatically scoped to their own company_key from JWT
      - User: not allowed
    """
    # Permission check: only Manager and above can list users
    caller_level, caller_role = get_caller_highest_role(groups)
    if caller_level < ROLE_HIERARCHY.get("Manager", 2):
        return build_response(403, {"error": "You do not have permission to view users."})

    # Determine which company to query
    query_params = event.get("queryStringParameters") or {}

    if "SuperAdmin" in groups:
        company_key = query_params.get("company_key", "").strip()
        if not company_key:
            return build_response(400, {"error": "SuperAdmins must provide '?company_key=xxx' query parameter."})
    else:
        company_key = caller_company_key
        if not company_key:
            return build_response(403, {"error": "Could not determine your company key."})

    try:
        # Use Cognito ListUsers with a filter on custom:company_key
        paginator_token = query_params.get("next_token", None)
        limit = min(int(query_params.get("limit", "60")), 60)

        list_kwargs = {
            "UserPoolId": user_pool_id,
            "Limit": limit,
        }
        if paginator_token:
            list_kwargs["PaginationToken"] = paginator_token

        response = cognito.list_users(**list_kwargs)

        users = []
        for user in response.get("Users", []):
            attrs = {a["Name"]: a["Value"] for a in user.get("Attributes", [])}
            if attrs.get("custom:company_key", "") != company_key:
                continue

            # Get the user's groups
            user_groups = []
            try:
                group_resp = cognito.admin_list_groups_for_user(
                    UserPoolId=user_pool_id,
                    Username=user["Username"],
                )
                user_groups = [g["GroupName"] for g in group_resp.get("Groups", [])]
            except Exception:
                pass  # Non-critical, continue without group info

            users.append({
                "username": user["Username"],
                "email": attrs.get("email", ""),
                "name": attrs.get("name", ""),
                "company_key": attrs.get("custom:company_key", ""),
                "groups": user_groups,
                "status": user.get("UserStatus", ""),
                "enabled": user.get("Enabled", False),
                "created_at": user.get("UserCreateDate", "").isoformat() if hasattr(user.get("UserCreateDate", ""), "isoformat") else str(user.get("UserCreateDate", "")),
            })

        result = {
            "users": users,
            "count": len(users),
            "company_key": company_key,
        }

        # Include pagination token if more results exist
        if "PaginationToken" in response:
            result["next_token"] = response["PaginationToken"]

        return build_response(200, result)

    except Exception as e:
        logger.exception("Failed to list users")
        return build_response(500, {"error": f"Failed to list users: {str(e)}"})


# ───────────────────────────────────────────────────────────────
# HANDLER: GET /api/users/{email}
# ───────────────────────────────────────────────────────────────
def handle_get_user(event, user_pool_id, groups, caller_company_key, path):
    """
    Get a single user's profile by email.
      - SuperAdmin: can view any user
      - Admin/Manager: can only view users from their own company
    """
    caller_level, caller_role = get_caller_highest_role(groups)
    if caller_level < ROLE_HIERARCHY.get("Manager", 2):
        return build_response(403, {"error": "You do not have permission to view user details."})

    # Extract email from path: /api/users/{email}
    parts = path.rstrip("/").split("/")
    if len(parts) < 4 or not parts[3]:
        return build_response(400, {"error": "Missing user email in path. Use /api/users/{email}"})

    target_email = parts[3]

    try:
        response = cognito.admin_get_user(
            UserPoolId=user_pool_id,
            Username=target_email,
        )

        attrs = {a["Name"]: a["Value"] for a in response.get("UserAttributes", [])}
        user_company_key = attrs.get("custom:company_key", "")

        # Tenant isolation: non-SuperAdmins can only view users from their own company
        if "SuperAdmin" not in groups and user_company_key != caller_company_key:
            return build_response(403, {"error": "You can only view users from your own company."})

        # Get groups
        user_groups = []
        try:
            group_resp = cognito.admin_list_groups_for_user(
                UserPoolId=user_pool_id,
                Username=target_email,
            )
            user_groups = [g["GroupName"] for g in group_resp.get("Groups", [])]
        except Exception:
            pass

        user_data = {
            "username": response.get("Username", ""),
            "email": attrs.get("email", ""),
            "name": attrs.get("name", ""),
            "company_key": user_company_key,
            "groups": user_groups,
            "status": response.get("UserStatus", ""),
            "enabled": response.get("Enabled", False),
            "created_at": response.get("UserCreateDate", "").isoformat() if hasattr(response.get("UserCreateDate", ""), "isoformat") else str(response.get("UserCreateDate", "")),
        }

        return build_response(200, user_data)

    except cognito.exceptions.UserNotFoundException:
        return build_response(404, {"error": f"User '{target_email}' not found."})
    except Exception as e:
        logger.exception("Failed to get user")
        return build_response(500, {"error": f"Failed to get user: {str(e)}"})


# ───────────────────────────────────────────────────────────────
# HANDLER: POST /api/users/invite
# ───────────────────────────────────────────────────────────────
def handle_invite_user(event, user_pool_id, groups, caller_company_key):
    """
    Invite (create) a new user in Cognito and assign to a group.
    """
    # Parse body
    body_str = event.get("body", "{}")
    try:
        body = json.loads(body_str) if isinstance(body_str, str) else body_str
    except Exception as e:
        return build_response(400, {"error": "Invalid JSON body"})

    # SuperAdmin override logic
    admin_company_key = caller_company_key
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

    # --- Validate the target group name ---
    if group_name not in ALLOWED_GROUPS:
        return build_response(400, {
            "error": f"Invalid group '{group_name}'. Allowed groups: {', '.join(sorted(ALLOWED_GROUPS))}"
        })

    # --- Validate invite permission based on role hierarchy ---
    is_allowed, reason = validate_invite_permission(groups, group_name)
    if not is_allowed:
        logger.warning(f"Permission denied: {reason} | caller_groups={groups}, target_group={group_name}")
        return build_response(403, {"error": reason})

    try:
        # Create the user in Cognito and automatically assign the company_key
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
                    'Name': 'profile',
                    'Value': group_name
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

        # Add user to the specified Cognito group
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
        # Check if the user never logged in (expired invite)
        try:
            existing = cognito.admin_get_user(UserPoolId=user_pool_id, Username=email)
            status = existing.get("UserStatus", "")
            if status == "FORCE_CHANGE_PASSWORD":
                return build_response(409, {
                    "error": "This user was already invited but hasn't logged in yet. Use the resend-invite endpoint to resend.",
                    "user_status": status,
                    "can_resend": True
                })
            else:
                return build_response(409, {"error": f"A user with this email already exists (status: {status})."})
        except Exception:
            return build_response(409, {"error": "A user with this email already exists."})
    except Exception as e:
        logger.exception("Failed to create user in Cognito")
        return build_response(500, {"error": f"Failed to create user: {str(e)}"})


# ───────────────────────────────────────────────────────────────
# HANDLER: POST /api/users/resend-invite
# ───────────────────────────────────────────────────────────────
def handle_resend_invite(event, user_pool_id, groups, caller_company_key):
    """
    Resend invite to a user whose temporary password has expired.
    This resets the temp password and sends a new invite email.
    Body: { "email": "user@example.com" }
    """
    # Permission check: only Manager and above can resend
    caller_level, caller_role = get_caller_highest_role(groups)
    if caller_level < ROLE_HIERARCHY.get("Manager", 2):
        return build_response(403, {"error": "You do not have permission to resend invites."})

    body_str = event.get("body", "{}")
    try:
        body = json.loads(body_str) if isinstance(body_str, str) else body_str
    except Exception:
        return build_response(400, {"error": "Invalid JSON body"})

    email = body.get("email", "").strip()
    if not email:
        return build_response(400, {"error": "Missing required field: email"})

    try:
        # 1. Verify the user exists and check their company_key
        existing = cognito.admin_get_user(UserPoolId=user_pool_id, Username=email)
        attrs = {a["Name"]: a["Value"] for a in existing.get("UserAttributes", [])}
        user_company_key = attrs.get("custom:company_key", "")
        user_status = existing.get("UserStatus", "")

        # Tenant isolation: non-SuperAdmins can only resend to their own company
        if "SuperAdmin" not in groups and user_company_key != caller_company_key:
            return build_response(403, {"error": "You can only resend invites to users in your own company."})

        # Only resend if the user hasn't completed sign-up yet
        if user_status not in ("FORCE_CHANGE_PASSWORD",):
            return build_response(400, {
                "error": f"Cannot resend invite. User status is '{user_status}'. Resend is only for users who haven't logged in yet."
            })

        # 2. Resend by calling AdminCreateUser with RESEND message action
        cognito.admin_create_user(
            UserPoolId=user_pool_id,
            Username=email,
            MessageAction="RESEND",
            DesiredDeliveryMediums=["EMAIL"],
        )

        logger.info(f"Successfully resent invite to {email}")

        return build_response(200, {
            "message": "Invite resent successfully",
            "email": email,
            "company_key": user_company_key
        })

    except cognito.exceptions.UserNotFoundException:
        return build_response(404, {"error": f"User '{email}' not found. Use the invite endpoint to create them first."})
    except Exception as e:
        logger.exception("Failed to resend invite")
        return build_response(500, {"error": f"Failed to resend invite: {str(e)}"})


# ───────────────────────────────────────────────────────────────
# HANDLER: DELETE /api/users/{email}
# ───────────────────────────────────────────────────────────────
def handle_delete_user(event, user_pool_id, groups, caller_company_key, path, claims):
    """
    Delete a user from Cognito.
    Rules:
      - Only Admin and above can delete users.
      - You can only delete roles BELOW your own.
      - Tenant isolation: non-SuperAdmins can only delete users from their own company.
      - You cannot delete yourself.
    """
    caller_level, caller_role = get_caller_highest_role(groups)
    if caller_level < ROLE_HIERARCHY.get("Admin", 3):
        return build_response(403, {"error": "Only Admins and above can delete users."})

    # Extract email from path: /api/users/{email}
    parts = path.rstrip("/").split("/")
    if len(parts) < 4 or not parts[3]:
        return build_response(400, {"error": "Missing user email in path. Use DELETE /api/users/{email}"})

    target_email = parts[3]

    # Prevent self-deletion
    caller_email = claims.get("email", "")
    if target_email.lower() == caller_email.lower():
        return build_response(400, {"error": "You cannot delete your own account."})

    try:
        # 1. Get the target user's details
        existing = cognito.admin_get_user(UserPoolId=user_pool_id, Username=target_email)
        attrs = {a["Name"]: a["Value"] for a in existing.get("UserAttributes", [])}
        user_company_key = attrs.get("custom:company_key", "")

        # 2. Tenant isolation
        if "SuperAdmin" not in groups and user_company_key != caller_company_key:
            return build_response(403, {"error": "You can only delete users from your own company."})

        # 3. Get the target user's groups to check role hierarchy
        target_groups = []
        try:
            group_resp = cognito.admin_list_groups_for_user(
                UserPoolId=user_pool_id,
                Username=target_email,
            )
            target_groups = [g["GroupName"] for g in group_resp.get("Groups", [])]
        except Exception:
            pass

        target_level, target_role = get_caller_highest_role(target_groups)

        # 4. Cannot delete users at or above your own level
        if target_level >= caller_level:
            return build_response(403, {
                "error": f"A {caller_role} cannot delete a {target_role or 'user'}. You can only delete roles below your own."
            })

        # 5. Delete the user
        cognito.admin_delete_user(
            UserPoolId=user_pool_id,
            Username=target_email
        )

        logger.info(f"Successfully deleted user {target_email} by {caller_email}")

        return build_response(200, {
            "message": "User deleted successfully",
            "email": target_email,
            "company_key": user_company_key
        })

    except cognito.exceptions.UserNotFoundException:
        return build_response(404, {"error": f"User '{target_email}' not found."})
    except Exception as e:
        logger.exception("Failed to delete user")
        return build_response(500, {"error": f"Failed to delete user: {str(e)}"})
