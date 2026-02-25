# ALJE Django API Project Documentation

> This comprehensive documentation outlines the architecture, database configurations, dependency requirements, and API structure for the ALJE system.

## 1. Project Overview
This Django backend serves as the core engine and API endpoint provider for the ALJE Flutter application. It efficiently manages advanced logistics operations, outbound distributions, inbound warehousing, multi-location mapping, and system-wide request validations through a structured REST architecture.

## 2. Database Models & App Distribution
The system utilizes three separate database connections to handle standard operations, inbound data processing, and user session management sequentially. The architecture relies on Django’s robust Object-Relational Mapping (ORM) and separates responsibilities across various localized applications inside the backend engine.

- **Total Database Connections:** 3 SQL Server Integrations
- **Total Custom Tables:** 78 tables
- **Total API Endpoints:** 592 API Routes

### App-Specific Distribution (Models, Views & API Endpoints)
The architecture splits database interactions between strict Object-Relational Models (ORM via `models.py`) and dynamic raw SQL queries injected within the Python Views (`views.py`):

- **`ALJE_APP` (Outbound Logistics):** 
  - **Tables Queried Directly:** 82 Tables (via Raw SQL)
  - **Total API Endpoints:** 214 API URLs
- **`New_Outbound_App` (Enhanced Outbound):** 
  - **Tables Queried Directly:** 50 Tables (via Raw SQL)
  - **Total API Endpoints:** 230 API URLs
- **`Inbound_App` (Receiving & Storage):** 
  - **Tables Queried Directly:** 16 Tables (via Raw SQL)
  - **Total API Endpoints:** 85 API URLs
- **`Location_Map_App` (Warehouse Tracking):** 
  - **Tables Queried Directly:** 12 Tables (via Raw SQL)
  - **Total API Endpoints:** 55 API URLs

## 3. Database Structure & Configurations
The project is connected to an array of SQL servers handling massive loads separately (as verified in the project's configurations). Django distributes the ORM queries via a designated default router over three specific endpoints.

- **Database 1 (Default):** Connected to the `BUYP` database (Primary DB for outbound logic and core).
- **Database 2:** Connected to `BUYP_INBOUND` (Secondary DB exclusively managing complex Inbound App logic).
- **Database 3:** Connected to `BUYP_SESSION` (Tertiary DB handling real-time Django and Flutter persistent sessions).

## 4. System Environment Analysis (.env Configurations)
The project heavily relies on secure `.env` configurations to decouple sensitive logic from source code. Below is a breakdown of the critical components handled within the actual `.env` file:

- **General Django Settings:** Configurations governing security and hosting paths (`DJANGO_SECRET_KEY`, `DEBUG`, `ALLOWED_HOSTS` covering physical IPs for flutter testing).
- **Multi-Database Configuration:** Defines 3 distinct server clusters:
  - **Primary MSSQL:** Hosts parameters like `MSSQL_DB=BUYP`, `MSSQL_USER`, `MSSQL_HOST=103.48.180.245`.
  - **Second Database (Inbound):** Uses `MSSQL_DB_2=BUYP_INBOUND`.
  - **Third Database (Sessions):** Uses `MSSQL_DB_3=BUYP_SESSION`.
- **MinIO S3 Integrations:** The backend is natively connected to a self-hosted S3 service (`http://192.168.10.131:9000`) functioning via Buckets:
  - `alje`, `internaldamage`
  - `inboundcontainerinfoalje`
  - `inboundalje`, `inboundpodetails`
  - `inboundcontainerqcimage`, `inboundproductdamage`
- **Additional Features:** Handles `DRF_USER_THROTTLE_RATE` internally mapping to 1000/minute to prevent mobile-app spamming.

## 5. Requirement Dependencies Analysis
The system relies on an optimal **27 required libraries** defined via `requirements.txt`. They form the building blocks of the API integration mechanism:

- **Core Architectures:** `Django==5.0.9` and `djangorestframework==3.15.2`. Provide routing and JSON serialization.
- **SQL Integrations:** `mssql-django==1.5`, `pyodbc==5.2.0`, and `sqlparse==0.5.3` handle complex database queries natively into Microsoft SQL systems without raw statements.
- **S3 Storage (MinIO) Systems:** `boto3`, `botocore`, and `minio==7.2.20` are required to handle asynchronous image/document upload handling from Flutter directly into the Buckets.
- **Data Extraction / Utilities:** Uses `openpyxl` for dynamically exporting dispatch logs or report screens into Excel files (like in `Outbound Fullfilled Dispatch`).
- **Security & Configurations:** Integrated with `django-cors-headers==4.4.0` handling Flutter CORS bypass natively and `django-environ==0.12.0` to manage the `.env` extraction securely.

## 6. API Specifications
The project acts as a massive data provider, utilizing HTTP routes strictly defined in `urls.py`.

- **Total API Endpoints Available:** 584 Core Feature API Routes.
- **Main Route Prefixes:** API architecture is cleanly split via the main urls connector, formatting requests into these primary routes:
  1. `http://<SERVER_IP>:<PORT>/Outbound/...` (Handles dispatch, trucks, and salesman queries)
  2. `http://<SERVER_IP>:<PORT>/NewOutbound/...` (Handles new enhanced outbound logic)
  3. `http://<SERVER_IP>:<PORT>/Inbound/...` (Handles warehouse receiving and PO storage)
  4. `http://<SERVER_IP>:<PORT>/Location_Mapping/...` (Handles internal location coordinates and dimensions)

## 7. Connecting Django APIs to Flutter
To connect your Flutter frontend to these 584 APIs and 36 backend models, follow this standard implementation guide using the Dart `http` package.

### Step 1: Add Dependency
In your Flutter project’s `pubspec.yaml`, add the HTTP client:
```yaml
dependencies:
  flutter:
    sdk: flutter
  http: ^1.2.0
```

### Step 2: Implementation Setup in Dart
Here is a professional boilerplate to handle GET (fetching tables) and POST (updating tables) requests securely from Flutter:

```dart
import 'dart:convert';
import 'package:http/http.dart' as http;

class DjangoApiService {
  // Replace with your Active Local Django Server IP (e.g., 192.168.10.110:8008)
  static const String baseUrl = "http://YOUR_SERVER_IP:8008"; 

  // 1. Fetching Data from Django (GET Request)
  Future<void> fetchOutboundDetails() async {
    final url = Uri.parse('$baseUrl/Outbound/get-employee-details/');
    
    try {
      final response = await http.get(
        url,
        headers: {
          'Content-Type': 'application/json',
        },
      );

      if (response.statusCode == 200) {
        var decodedJson = jsonDecode(response.body);
        print("Data Successfully fetched from Django! $decodedJson");
      } else {
        print("API Error: ${response.statusCode}");
      }
    } catch (error) {
      print("Network Error: $error");
    }
  }

  // 2. Sending Data to Django (POST Request)
  Future<void> submitInboundCargo(Map<String, dynamic> payload) async {
    final url = Uri.parse('$baseUrl/Inbound/save-location/');
    
    try {
      final response = await http.post(
        url,
        headers: {'Content-Type': 'application/json'},
        body: jsonEncode(payload),
      );

      if (response.statusCode == 200 || response.statusCode == 201) {
        print("Success! Data written to the Django Tables");
      } else {
        print("Failed to push data: ${response.body}");
      }
    } catch (error) {
      print("Network Error: $error");
    }
  }
}
```

### Best Workflow Practices for Flutter integration:
1. Always utilize `jsonDecode()` when pulling complex data from Django.
2. Structure your Flutter models to mirror the Django ORM fields exactly to prevent null errors.
3. Use your physical machine's local network IP configuration (like `192.168.x.x`) for mobile-to-local testing instead of `127.0.0.1`.
