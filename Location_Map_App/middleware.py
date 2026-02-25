from django.shortcuts import redirect
from django.db import connections
from django.utils import timezone
from .utils import get_session_config
import datetime
 
class SingleSessionMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response


    def _handle_unauthorized(self, request, reason="Session invalid"):
        if request.path.startswith('/api/'):
            from django.http import JsonResponse
            return JsonResponse({'status': 'error', 'message': 'Unauthorized', 'reason': reason}, status=401)
        return redirect('login')
 
    def __call__(self, request):
        if request.user.is_authenticated or 'user_id' in request.session:
            # 1. Single Session Check
            username = request.session.get('username')
            session_token = request.session.get('session_token')
 
            if username and session_token:
                try:
                     with connections['session_db'].cursor() as cursor:
                        # 1. Single Session Check
                        cursor.execute("SELECT session_token FROM [BUYP_SESSION].[dbo].[active_sessions] WHERE username = %s", [username])
                        result = cursor.fetchone()
                       
                        if result:
                            db_token = result[0]
                            # Handle potential type mismatch if db_token is not string
                            if str(db_token) != str(session_token):
                                # Session mismatch - force logout
                                request.session.flush()
                                return self._handle_unauthorized(request, "Session token mismatch")
                        else:
                             request.session.flush()
                             return self._handle_unauthorized(request, "Session not found in DB")
                             
                        # 2. Timeout Checks
                        config = get_session_config()
                        now = datetime.datetime.now().timestamp()
                       
                        # Check Idle Timeout
                        last_activity = request.session.get('last_activity')
                        if last_activity:
                             if (now - last_activity) > config['idle_timeout']:
                                 cursor.execute("DELETE FROM [BUYP_SESSION].[dbo].[active_sessions] WHERE username = %s", [username])
                                 request.session.flush()
                                 return self._handle_unauthorized(request, "Idle timeout")
                       
                        # Check Absolute Lifetime
                        session_start = request.session.get('session_start_time')
                        if session_start:
                             if (now - session_start) > config['max_lifetime']:
                                 cursor.execute("DELETE FROM [BUYP_SESSION].[dbo].[active_sessions] WHERE username = %s", [username])
                                 request.session.flush()
                                 return self._handle_unauthorized(request, "Max lifetime exceeded")
                             
                     # Update Activity safely
                     request.session['last_activity'] = now
                     
                except Exception:
                    pass
       
        response = self.get_response(request)
        return response