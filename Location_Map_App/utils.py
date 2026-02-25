from django.db import connections

def get_session_config():
    try:
        with connections['session_db'].cursor() as cursor:
            cursor.execute("SELECT idle_timeout_seconds, max_lifetime_seconds FROM [BUYP_SESSION].[dbo].[session_config] WHERE id = 1")
            row = cursor.fetchone()
            if row:
                return {
                    'idle_timeout': row[0],
                    'max_lifetime': row[1]
                }
    except Exception:
        pass
    
    # Defaults
    return {
        'idle_timeout': 600, # 10 minutes
        'max_lifetime': 10800 # 3 hours
    }



import ipaddress
 
def get_client_ip(request):
    x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
    if x_forwarded_for:
        ip = x_forwarded_for.split(',')[0]
    else:
        ip = request.META.get('REMOTE_ADDR')
    return ip