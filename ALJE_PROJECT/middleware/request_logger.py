# import logging
# from datetime import datetime
# from collections import Counter
# from django.utils.timezone import now

# from ALJE_APP.models import RequestLog  # adjust if in different app

# logger = logging.getLogger("request_logger")

# request_stats = {
#     'total_requests': 0,
#     'path_counter': Counter(),
#     'ip_set': set(),
#     'errors': [],
#     'recent_requests': []
# }

# class RequestLoggingMiddleware:
#     def __init__(self, get_response):
#         self.get_response = get_response

#     def __call__(self, request):
#         start_time = datetime.now()
#         response = None
#         error_message = None

#         try:
#             response = self.get_response(request)
#         except Exception as e:
#             error_message = str(e)
#             status = 500
#             response = self.handle_exception(request, e)
#         finally:
#             end_time = datetime.now()

#         duration = (end_time - start_time).total_seconds() * 1000
#         timestamp = start_time.strftime('%Y-%m-%d %H:%M:%S')
#         ip = request.META.get('REMOTE_ADDR', 'unknown')
#         path = request.path
#         status = response.status_code
#         content_size = len(response.content) / 1024  # in KB

#         # Save in memory
#         request_stats['total_requests'] += 1
#         request_stats['path_counter'][path] += 1
#         request_stats['ip_set'].add(ip)
#         request_stats['recent_requests'].append({
#             'time': timestamp,
#             'ip': ip,
#             'path': path,
#             'method': request.method,
#             'status': status,
#             'size_kb': f"{content_size:.2f}",
#             'duration_ms': f"{duration:.2f}",
#             'error': error_message,
#         })
#         request_stats['recent_requests'] = request_stats['recent_requests'][-50:]

#         if error_message:
#             request_stats['errors'].append({
#                 'time': timestamp,
#                 'path': path,
#                 'error': error_message
#             })
#             request_stats['errors'] = request_stats['errors'][-10:]

#         # Save to DB
#         RequestLog.objects.create(
#             timestamp=now(),
#             ip=ip,
#             path=path,
#             method=request.method,
#             status=status,
#             size_kb=round(content_size, 2),
#             duration_ms=round(duration, 2),
#             error=error_message
#         )

#         return response

#     def handle_exception(self, request, exception):
#         from django.http import HttpResponseServerError
#         return HttpResponseServerError("Internal Server Error")
