from multiprocessing.dummy import connection
from django.shortcuts import render, redirect,  get_object_or_404
from django.db import connections
from django.contrib import messages
from django.http import HttpResponse, JsonResponse
from django.urls import reverse
import uuid
import datetime
import json
from ..utils import get_session_config, get_client_ip
from django.views.decorators.csrf import csrf_exempt
from django.db import IntegrityError

def signup_view(request):
    if request.method == 'POST':
        name = request.POST.get('name')
        username = request.POST.get('username')
        password = request.POST.get('password')

        if not name or not username or not password:
             return render(request, 'signup.html', {'error': 'All fields are required.'})

        try:
            with connections['session_db'].cursor() as cursor:
                # Check if username already exists
                cursor.execute("SELECT count(*) FROM [BUYP_SESSION].[dbo].[signup] WHERE Username = %s", [username])
                if cursor.fetchone()[0] > 0:
                     return render(request, 'signup.html', {'error': 'Username already exists.'})

                # Insert new user
                cursor.execute("INSERT INTO [BUYP_SESSION].[dbo].[signup] ([Name], [Username], [Password]) VALUES (%s, %s, %s)", [name, username, password])
                
                # Insert into login table for consistent auth check
                cursor.execute("INSERT INTO [BUYP_SESSION].[dbo].[Session_User_Login_Details_tbl] ([username], [Password]) VALUES (%s, %s)", [username, password])

            messages.success(request, 'Account created successfully! Please login.')
            return redirect('login')
        except Exception as e:
            return render(request, 'signup.html', {'error': f'An error occurred: {e}'})

    return render(request, 'signup.html')

def login_view(request):
    # Compliance with "Same Browser" rule:
    # If the dashboard is already open (valid session), skip login and go directly to dashboard.
    if request.session.get('user_id') and request.session.get('session_token'):
        return redirect('dashboard')

    if request.method == 'POST':
        username = request.POST.get('username')
        password = request.POST.get('password')
        confirm_force = request.POST.get('confirm_force')
        action_type = request.POST.get('action_type')

        if not username or not password:
            return render(request, 'login.html', {'error': 'Username and Password are required.'})

        try:
            with connections['session_db'].cursor() as cursor:
                # Check against Login table
                cursor.execute("SELECT * FROM [BUYP_SESSION].[dbo].[Session_User_Login_Details_tbl] WHERE username = %s AND Password = %s", [username, password])
                user = cursor.fetchone()
                
                if user:
                    # User authenticated.
                    


                    # Check for active session.
                    active_session = None
                    cursor.execute("SELECT session_token FROM [BUYP_SESSION].[dbo].[Login_Active_Session_tbl] WHERE username = %s", [username])
                    active_session = cursor.fetchone()
                        
                    if active_session and not confirm_force:
                        # Conflict detected!
                        # We don't log them in yet. We show the popup.
                         return render(request, 'login.html', {
                             'error': 'Session Conflict',
                             'conflict': True,
                             'username_value': username, # Pass back to refill form
                             'password_value': password  # Pass back to refill form (hidden)
                         })
                    
                    # Proceed with Login (First time or Forced)
                    new_token = str(uuid.uuid4())
                    now = datetime.datetime.now().timestamp()
                    
                    # Update or Insert active session
                    if active_session:
                        cursor.execute("UPDATE [BUYP_SESSION].[dbo].[Login_Active_Session_tbl] SET session_token = %s, last_login = GETDATE() WHERE username = %s", [new_token, username])
                    else:
                        cursor.execute("INSERT INTO [BUYP_SESSION].[dbo].[Login_Active_Session_tbl] (username, session_token) VALUES (%s, %s)", [username, new_token])
                    
                    # Set session
                    request.session['user_id'] = user[0] 
                    request.session['username'] = username
                    request.session['session_token'] = new_token
                    
                    # New Session Timestamps for Timeout Middleware
                    request.session['session_start_time'] = now
                    request.session['last_activity'] = now
                    
                    return redirect('dashboard')
                else:
                    return render(request, 'login.html', {'error': 'Invalid username or password.'})
        except Exception as e:
             return render(request, 'login.html', {'error': f'Database error: {e}'})

    return render(request, 'login.html')

def dashboard_view(request):
    if 'user_id' not in request.session:
        return redirect('login')
    
    # Pass config to template for JS timeout handling
    config = get_session_config()
    
    return render(request, 'dashboard.html', {
        'username': request.session.get('username'),
        'idle_timeout': config['idle_timeout'],
        'max_lifetime': config['max_lifetime']
    })

def logout_view(request):
    try:
        username = request.session.get('username')
        if username:
            with connections['session_db'].cursor() as cursor:
                 cursor.execute("DELETE FROM [BUYP_SESSION].[dbo].[Login_Active_Session_tbl] WHERE username = %s", [username])
    except Exception:
        pass

    request.session.flush()



    return redirect('login')

# --- TAB HANDLING APIs ---

def check_tab_session(request):
    """
    Checks if the provided tab_id matches the active session's tab ID.
    If no tab is registered yet for this session, it registers it (First Tab).
    """
    if not request.user.is_authenticated and 'user_id' not in request.session:
         return JsonResponse({'valid': False, 'reason': 'not_logged_in'}, status=401)
         
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            tab_id = data.get('tab_id')
            
            current_active_tab = request.session.get('active_tab_id')
            
            if not current_active_tab:
                # First tab to claim session
                request.session['active_tab_id'] = tab_id
                request.session.modified = True
                return JsonResponse({'valid': True, 'status': 'registered'})
            
            if current_active_tab == tab_id:
                return JsonResponse({'valid': True, 'status': 'active'})
            else:
                return JsonResponse({'valid': False, 'status': 'conflict'})
                
        except Exception as e:
            return JsonResponse({'valid': False, 'error': str(e)}, status=500)
    
    return JsonResponse({'valid': False}, status=400)

def activate_tab_session(request):
    """
    Force activates the current tab, overriding any previous tab.
    """
    if not request.user.is_authenticated and 'user_id' not in request.session:
         return JsonResponse({'valid': False}, status=401)
         
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            tab_id = data.get('tab_id')
            
            request.session['active_tab_id'] = tab_id
            request.session.modified = True
            return JsonResponse({'success': True})
        except Exception as e:
            return JsonResponse({'success': False}, status=500)
            
    return JsonResponse({'success': False}, status=400)
def session_heartbeat(request):
    """
    Heartbeat to check validity of session.
    Prioritizes X-Session-Token (Flutter App) for robustness.
    Fallbacks to Session Cookie (Browser).
    """
    # Prioritizes Authorization header (Standard) or X-Session-Token (Legacy)
    token = None
    auth_header = request.headers.get('Authorization')
    if auth_header:
        parts = auth_header.split()
        if len(parts) == 2 and parts[0].lower() == 'bearer':
            token = parts[1]
            
    if not token:
        token = request.headers.get('X-Session-Token')
    
    if token:
        try:
            from django.db import connections
            with connections['session_db'].cursor() as cursor:
                 cursor.execute("SELECT count(*) FROM [BUYP_SESSION].[dbo].[Login_Active_Session_tbl] WHERE session_token = %s", [token])
                 if cursor.fetchone()[0] > 0:
                     return JsonResponse({'status': 'active'})
                 else:
                     return JsonResponse({'status': 'error', 'message': 'Invalid Token'}, status=401)
        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)}, status=500)

    # Fallback to Django Session Cookie
    if 'user_id' not in request.session:
        return JsonResponse({'status': 'error', 'message': 'Unauthorized'}, status=401)
    
    return JsonResponse({'status': 'active'})


# --- FLUTTER APIs ---

@csrf_exempt
def api_signup(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            name = data.get('name')
            username = data.get('username')
            password = data.get('password')

            if not name or not username or not password:
                return JsonResponse({'status': 'error', 'message': 'All fields are required.'}, status=400)

            with connections['session_db'].cursor() as cursor:
                # Check if username already exists
                cursor.execute("SELECT count(*) FROM [BUYP_SESSION].[dbo].[signup] WHERE Username = %s", [username])
                if cursor.fetchone()[0] > 0:
                     return JsonResponse({'status': 'error', 'message': 'Username already exists.'}, status=409)

                # Insert new user
                cursor.execute("INSERT INTO [BUYP_SESSION].[dbo].[signup] ([Name], [Username], [Password]) VALUES (%s, %s, %s)", [name, username, password])
                
                # Insert into login table
                cursor.execute("INSERT INTO [BUYP_SESSION].[dbo].[Session_User_Login_Details_tbl] ([username], [Password]) VALUES (%s, %s)", [username, password])

            return JsonResponse({'status': 'success', 'message': 'Account created successfully'})
        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)}, status=500)
    return JsonResponse({'status': 'error', 'message': 'Method not allowed'}, status=405)

@csrf_exempt
def api_login(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            username = data.get('username')
            password = data.get('password')
            confirm_force = data.get('confirm_force', False) # Boolean
            
            if not username or not password:
                 return JsonResponse({'status': 'error', 'message': 'Username and Password are required.'}, status=400)

            with connections['session_db'].cursor() as cursor:
                # Check credentials
                cursor.execute("SELECT * FROM [BUYP_SESSION].[dbo].[Session_User_Login_Details_tbl] WHERE username = %s AND Password = %s", [username, password])
                user = cursor.fetchone()
                
                if user:
                    # Check for active session
                    # We check IF existing session exists AND we are not forcing.
                    if not confirm_force:
                         cursor.execute("SELECT session_token FROM [BUYP_SESSION].[dbo].[Login_Active_Session_tbl] WHERE username = %s", [username])
                         active_session = cursor.fetchone()
                         if active_session:
                             return JsonResponse({
                                 'status': 'conflict', 
                                 'message': 'Active Session Detected',
                                 'username': username
                             }, status=409)

                    # Proceed with Login (New or Forced)
                    new_token = str(uuid.uuid4())
                    now = datetime.datetime.now().timestamp()
                    
                    # Upsert session
                    # We can try to update, if 0 rows detected then insert? Or check first.
                    # Given we might have deleted it or it might exist, let's just check existence again or use MERGE syntax if SQL Server, but let's stick to simple logic used before.
                    
                    cursor.execute("SELECT count(*) FROM [BUYP_SESSION].[dbo].[Login_Active_Session_tbl] WHERE username = %s", [username])
                    exists = cursor.fetchone()[0] > 0
                    
                    if exists:
                        cursor.execute("UPDATE [BUYP_SESSION].[dbo].[Login_Active_Session_tbl] SET session_token = %s, last_login = GETDATE() WHERE username = %s", [new_token, username])
                    else:
                        cursor.execute("INSERT INTO [BUYP_SESSION].[dbo].[Login_Active_Session_tbl] (username, session_token) VALUES (%s, %s)", [username, new_token])
                    
                    # Set Session Cookies
                    request.session['user_id'] = user[0] 
                    request.session['username'] = username
                    request.session['session_token'] = new_token
                    request.session['session_start_time'] = now
                    request.session['last_activity'] = now
                    
                    return JsonResponse({
                        'status': 'success', 
                        'message': 'Login successful',
                        'session_token': new_token,
                        'username': username
                    })
                else:
                    return JsonResponse({'status': 'error', 'message': 'Invalid username or password.'}, status=401)
        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)}, status=500)
    return JsonResponse({'status': 'error', 'message': 'Method not allowed'}, status=405)

@csrf_exempt
def api_logout(request):
    if request.method != "POST":
        return JsonResponse(
            {"status": "error", "message": "POST method required"},
            status=405
        )

    try:
        body = json.loads(request.body.decode("utf-8"))
        username = body.get("username")

        if not username:
            return JsonResponse(
                {"status": "error", "message": "username is required"},
                status=400
            )

        with connections['session_db'].cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM [BUYP_SESSION].[dbo].[Login_Active_Session_tbl]
                WHERE username = %s
                """,
                [username]
            )

        # Optional: clear session if exists
        request.session.flush()

        return JsonResponse({
            "status": "success",
            "message": "Logged out and session removed"
        })

    except Exception as e:
        return JsonResponse(
            {"status": "error", "message": str(e)},
            status=500
        )
    
def update_config_view(request):
    if not request.user.is_authenticated and 'user_id' not in request.session:
         return redirect('login')
         
    if request.method == 'POST':
        try:
            # Idle Calc
            idle_h = int(request.POST.get('idle_h', 0))
            idle_m = int(request.POST.get('idle_m', 0))
            idle_s = int(request.POST.get('idle_s', 0))
            idle_val = (idle_h * 3600) + (idle_m * 60) + idle_s

            # Max Life Calc
            max_h = int(request.POST.get('max_h', 0))
            max_m = int(request.POST.get('max_m', 0))
            max_s = int(request.POST.get('max_s', 0))
            max_val = (max_h * 3600) + (max_m * 60) + max_s
            
            with connections['session_db'].cursor() as cursor:
                cursor.execute("UPDATE [BUYP_SESSION].[dbo].[Session_Config_Setup_tbl]] SET idle_timeout_seconds = %s, max_lifetime_seconds = %s WHERE id = 1", [idle_val, max_val])
            
            messages.success(request, 'Configuration updated successfully!')
        except Exception as e:
            messages.error(request, f'Update failed: {e}')
            
    current_config = get_session_config()
    
    # Helper to split seconds
    def get_hms(seconds):
        h = seconds // 3600
        m = (seconds % 3600) // 60
        s = seconds % 60
        return h, m, s

    i_h, i_m, i_s = get_hms(current_config['idle_timeout'])
    m_h, m_m, m_s = get_hms(current_config['max_lifetime'])

    return render(request, 'config.html', {
        'idle_h': i_h, 'idle_m': i_m, 'idle_s': i_s,
        'max_h': m_h, 'max_m': m_m, 'max_s': m_s
    })


#--------------------------------------------------------------------------------------------------------------------------------------
# End of File
#--------------------------------------------------------------------------------------------------------------------------------------


@csrf_exempt
def login_dynamic(request):
    """
    Handles dynamic login operations based on 'type' (insert/update).
    """
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            op_type = data.get('type')
            username = data.get('username')
            password = data.get('password')
 
            if not op_type or not username or not password:
                return JsonResponse({'status': 'error', 'message': 'Type, username, and password are required.'}, status=400)
 
            client_ip = get_client_ip(request)
 
            with connections['session_db'].cursor() as cursor:
                if op_type == 'insert':
                    # Check if username already exists (strict username check)
                    cursor.execute("SELECT count(*) FROM [BUYP_SESSION].[dbo].[Session_User_Login_Details_tbl] WHERE username = %s", [username])
                    if cursor.fetchone()[0] > 0:
                         return JsonResponse({'status': 'error', 'message': 'Already logged in'}, status=400)
                   
                    # Insert new entry
                    cursor.execute(
                        "INSERT INTO [BUYP_SESSION].[dbo].[Session_User_Login_Details_tbl] ([username], [Password], [Ip]) VALUES (%s, %s, %s)",
                        [username, password, client_ip]
                    )
                    return JsonResponse({'status': 'success', 'message': 'User inserted into login table'})
 
                elif op_type == 'update':
                    # Update Logic: Update password and IP for the given username
                    # Note: The prompt implies updating based on username alone.
                   
                    cursor.execute(
                        "UPDATE [BUYP_SESSION].[dbo].[Session_User_Login_Details_tbl] SET [Password] = %s, [Ip] = %s WHERE [username] = %s",
                        [password, client_ip, username]
                    )
                   
                    if cursor.rowcount > 0:
                         return JsonResponse({'status': 'success', 'message': 'User updated successfully'})
                    else:
                         return JsonResponse({'status': 'error', 'message': 'User not found to update'}, status=404)
 
                else:
                    return JsonResponse({'status': 'error', 'message': 'Invalid operation type'}, status=400)
 
        except json.JSONDecodeError:
             return JsonResponse({'status': 'error', 'message': 'Invalid JSON'}, status=400)
        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)}, status=500)
 
    return JsonResponse({'status': 'error', 'message': 'Method not allowed'}, status=405)


def get_idle_timeout_seconds(request):
    try:
        with connections['session_db'].cursor() as cursor:
            cursor.execute("""
                SELECT TOP 1 idle_timeout_seconds
                FROM [BUYP_SESSION].[dbo].[Session_Config_Setup_tbl]
                ORDER BY id DESC
            """)
            row = cursor.fetchone()

        if row:
            return JsonResponse({
                "status": "success",
                "idle_timeout_seconds": row[0]
            })
        else:
            return JsonResponse({
                "status": "error",
                "message": "No data found"
            }, status=404)

    except Exception as e:
        return JsonResponse({
            "status": "error",
            "message": str(e)
        }, status=500)