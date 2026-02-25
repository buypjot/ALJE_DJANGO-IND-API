from django.urls import path, include,re_path

from . import views
from . import models
from rest_framework import routers
from Inbound_App.views import *
from django.conf import settings
from django.conf.urls.static import static

router = routers.DefaultRouter()

urlpatterns = [
    path("", include(router.urls)),    
   # For Inbound Concept url 
    path('save-location/', save_warehouse_location, name='save-location'),
    path('get-locations/', get_warehouse_locations, name='get-locations'),
    path('get-location-options/', get_location_options, name='get-location-options'),
    path('delete-location/', delete_warehouse_location, name='delete-location'),
    path('get_pending_po/', get_pending_po, name='get_pending_po'),
    path('supplier/<str:vendor_number>/', get_supplier_details ,name='supplier'),
    path('save_inbound_bayan/', InboundBayanFullView.as_view()),  # ✅ fix here
    path('generate_document_no/', views.generate_document_no, name='generate_document_no'),
    path('generate_PickId/', Pick_IdView.as_view(), name='generate_PickId'),
    path('save_container_Info/', SaveContainerInfoView.as_view()),  # ✅ fix here
    path('save_product_info/', SaveProductInfoView.as_view()),  # ✅ fix here
    path('save_expense_details/', SaveExpenseView.as_view()),  # ✅ fix here
    path('get_expense_cat/', get_expense_cat,name='get_expense_cat'),
    path('get_names_by_cat/<str:cat>/', get_names_by_expense_cat,name='get_names_by_cat'),
    path('Pick_ID_generate-token/', GeneratePick_IdView.as_view(), name='Pick_ID_generate-token'),
    path('get_inbound-shipments/', get_inbound_shipment_report, name='inbound-shipments'),
    path('get_existing_dOC_numbers/', get_existing_dOC_numbers, name='get_existing_dOC_numbers'), 
    path('get_existing_bayan_numbers/', get_existing_bayan_numbers, name='get_existing_bayan_numbers'),
    path('get_supplier_by_DocNo/<str:doc_no>/', get_supplier_by_DocNo, name='get_supplier_by_DocNo'),
    path('get_inbound-expenses/', views.get_inbound_expenses, name='get_inbound_expenses'),
    path('get_expense_filters/', views.get_expense_filters, name='get_expense_filters'),
    path("delete_expense/", views.delete_expense, name="delete_expense"),
    path("update_expense/", views.update_expense, name="update_expense"),
    path('get_inbound_products', views.get_inbound_products, name='get_inbound_products'),
    path('get_all_bayan_details/', views.get_all_bayan_details, name='get_all_bayan_details'),
    path('get_products_by_docno/', views.get_products_by_docno, name='get_products_by_docno'),
    path('update_truck_details/', views.update_truck_details, name='update_truck_details'),
    path('save_pickman_assignments/', views.update_pickman_assignments, name='update_pickman_assignments'),
    path('get_pickmenlist/', get_pickmenlist),
    path('get_pickmandetails/<str:pickman_name>/', pickman_details),
    path('save_pickman_data/', save_pickman_data),
    path('get_bayan_report_data/', get_pickman_BAYAN_data),
    path('get_inboundshipments_docNo/<str:doc_no>/', get_inbound_shipment_docNo_details),
    path('update_manager_status/', update_manager_status),
    path('update_superuser_status/', update_superuser_status),
    path('product_part_by_Po/', ProductPartsByPOView.as_view(), name='product_part_by_Po'),
    path('check_po_exists/<str:po_number>/', check_po_exists),
    path('get_pickmen_for_products/', get_pickmen_for_products),
    path('update-scanned-qty/', update_scanned_qty),
    path('update_sparesreceiver_status/', update_sparesreceiver_status),
    path('update_productreceiver_status/', update_productreceiver_status),
    path('get-item-description/<str:item_code>/', get_item_description),
    path('get_unique_supplier/', UniqueSupplierListView.as_view(), name='get_unique_supplier'),
    path('get_fully_scanned_pickman_details/', get_all_fully_scanned_pickman_details),
    path("save_currency/", save_currency, name="save_currency"),
    path("get_currencies/", get_currencies, name="get_currencies"),
    path("delete_currency/<int:currency_id>/", views.delete_currency, name="delete_currency"),
    path('BayanDoc_upload/', InboundMinioUploadView.as_view(), name='bayan-upload'),
    path('bayan-documents/<str:bayan_no>/', BayanDocumentsListView.as_view(), name='bayan-documents-list'),
    path('bayan-document-download/<int:document_id>/', BayanDocumentDownloadView.as_view(), name='bayan-document-download'),
    path('bayan-document-preview/<int:document_id>/', BayanDocumentPreviewView.as_view(), name='bayan-document-preview'),
    path("bayan-document-delete/<int:document_id>/", BayanDocumentDeleteView.as_view(), name="bayan-document-delete"),
    path('expense-categories/', views.expense_categories, name='expense-categories'),
    path('expense-categories/<int:id>/', views.expense_category_detail, name='expense-category-detail'),
    path('expense-names/', views.expense_names, name='expense-names'),
    path('expense-names/<int:id>/', views.expense_name_detail, name='expense-name-detail'),
    path('container_mino_excel_upload/', views.ExcelContainerUploadView.as_view(), name='container_mino_excel_upload'),
    path('document_container_report', ExcelContainerReportView.as_view(), name='document_container_report'),
    # path('document_container_report_all', ExcelContainerReportAllView.as_view(), name='document_container_report_all'),
    path('update_po_header/', UpdatePOHeaderView.as_view(), name='update_po_header'),
    path('download_document/<int:document_id>/', DownloadDocumentView.as_view(), name='download_document'),
    path('edit_container_document/', EditContainerDocumentView.as_view(), name='edit_container_document'),
    path('update_shipped_qty/', views.update_shipped_qty, name='update_shipped_qty'),
    path('get_shipment-details-by-docno/', views.get_shipment_details_by_docno, name='shipment_details_by_docno'),
    path('save_po_documents_minio/', save_po_documents_minio, name='save_po_documents_minio'),
    path('get_inbound_onProgress_data/', InboundOnProgressDataView.as_view(), name='get_inbound_onProgress_data'),
    path('get_all_inbound_gatepass/', views.get_all_inbound_gatepass, name='get_all_inbound_gatepass'),
    path('get_inbound_gatepass/<str:gatepass_no>/', views.get_inbound_gatepass_by_id, name='get_inbound_gatepass_by_id'),
    path('get_container_details/', views.get_container_data, name='get_bayan_container_data'),
    path('insert_GatePass_data/', views.insert_inbound_gatepass, name='insert_GatePass_data'),
    path('generate_gate_pass/', views.generate_gate_pass, name='generate_gate_pass'),    
    path('inbound_product_damage_insert/', views.inbound_product_damage_insert, name='inbound_product_damage_insert'),
    path('inbound_container_qc_insert/', views.inbound_container_qc_insert, name='inbound-container-qc-insert'),
    path('upload_container_images_to_minio/', views.upload_container_images_to_minio, name='upload_container_images_to_minio'),
    path('get-product-barcode/', views.get_product_barcode, name='get_product_barcode'),
    path('inbound_product_scanning_insert/', views.inbound_product_scanning_insert, name='inbound_product_scanning_insert'),
    path('upload-product-damage-images/', views.upload_product_damage_images_to_minio, name='upload_product_damage_images'),
    path('check_wh_transportation_status/', views.check_wh_transportation_status, name='check_wh_transportation_status'),
    path('get_consolidated_gatepass_data/', views.get_consolidated_gatepass_data, name='get_consolidated_gatepass_data'),  
    path('get_consolidated_gatepass_data_today/', views.get_consolidated_gatepass_data_today, name='get_consolidated_gatepass_data_today'),
    path('get_received_qty/', views.get_received_qty, name='get_received_qty'),
    path('get_damage_qty/',  views.get_damage_qty, name='get_damage_qty'),
    path('check_po_balance/', views.check_po_balance, name='check_po_balance'),  
    path('product_scan_check/', ProductScanCheckView.as_view(), name='product-scan-check'),
    path('inventory-history/', views.get_item_tracking_history, name='inventory-history'),
    path('create_vehicle_type/', views.create_vehicle_type, name='create_vehicle_type'),
    path('get_vehicle_types/', views.get_vehicle_types, name='get_vehicle_types'),
]

if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)