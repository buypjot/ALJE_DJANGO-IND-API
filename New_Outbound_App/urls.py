
from .views import finance_tema_View
from django.urls import path, include,re_path
from . import views
# from .views import log_view,log_detail
from . import models
from rest_framework import routers
from New_Outbound_App.views import *

router = routers.DefaultRouter()

router.register("User_member_details", User_member_detailsView, basename="User_member_details")
router.register("Physical_Warehouse", Physical_WarehouseView, basename="Physical_WarehouseView")
router.register("Salesman_List", Salesman_ListView, basename="Salesman_List")
router.register("Salesmandetails", SalesmandetailsView, basename="Salesmandetails")
router.register("UndeliveredData", UndeliveredDataView, basename="UndeliveredData")
router.register("loginsalesmanwarehousedetails", loginsalesmanwarehousedetailsView, basename="loginsalesmanwarehousedetails")
router.register("invoicedetails", InvoiceDetailsView, basename="invociedetails")
router.register("Create_Dispatch", Create_DispatchView, basename="Create_Dispatch")
# router.register("OnProgress_DispatchView", OnProgress_DispatchView, basename="OnProgress_DispatchView")
router.register("Dispatch_request", Dispatch_requestView, basename="Dispatch_request")
router.register("Pickman_scan", Pickman_scanView, basename="Pickman_scan")
router.register("Scanned_Pickman", Scanned_PickmanView, basename="Scanned_Pickman")
router.register("Truck_scan", Truck_scanView, basename="Truck_scan")
router.register("Shiment_Dispatch", Shiment_DispatchView, basename="Shiment_Dispatch")
router.register("ToGetGenerateDispatchView", ToGetGenerateDispatchView, basename="ToGetGenerateDispatchView")
router.register(r'employee-details', EmployeeView, basename='employee')
router.register("ProductcodeGet", ProductcodeGetView, basename="ProductcodeGet")
router.register("LogReports", LogReportsView, basename="LogReportsView")
router.register("TransactionDetail", TransactionDetailView, basename="TransactionDetail")
router.register("Return_dispatch", Return_dispatchView, basename="Return_dispatch")
# router.register("CreateDispatchWithTruckScanVieEEEEEEw", CreateDispatchWithTruckScanView, basename="CreateDispatchWithTruckScanView")
# router.register("Filter_StagingReport", Filter_StagingReportsView, basename="Filter_StagingReport")
# router.register("Shopinfo", ShopinfoView, basename="Shopinf")
router.register(
   r"InvoiceReportsUndeliveredDataView/(?P<status>\w+)/(?P<salesman_no>\d*)/(?P<columnname>[^/.]+)/(?P<columnvalue>[^/.]+)/(?P<fromdate>\d{4}-\d{2}-\d{2})/(?P<todate>\d{4}-\d{2}-\d{2})",
    InvoiceReportsUndeliveredDataView,
    basename="InvoiceReportsUndeliveredDataView"

    
)

router.register(r'Shipment_detialsView', Shipment_detialsView, basename='shipment-details')




router.register("DepartmentView", DepartmentDashboardView, basename="DepartmentView")

router.register("Departments", DepartmentView, basename="Departments")
router.register("DepRoles", DepRolesView, basename="DepRoles")
router.register("DepRoleForms", DepRoleFormsView, basename="DepRoleForms")
router.register("SaveUserDepAccess", DepUserManagementView, basename="SaveUserDepAccess")  

urlpatterns = [
    path("", include(router.urls)),

    path('process_dispatch_request/', views.process_dispatch_request, name='process_dispatch_request'),
    path('update-User_managemnet/<int:user_id>/', update_user_details),
    
    path('get-employee-details/', GetEmployeeDetailsView.as_view(), name='get-employee-details'),

    path('update_user_password_User_managemnet/<int:user_id>/', update_user_password),
    
    path('create_user_details_User_Management/', create_user_details),
 
 
        path('update-truck-flag/<int:id>/', update_truck_flag, name='update_truck_flag'),
        path('update_pickman_flag/<str:reqno>/<str:pickid>/<str:invoiceno>/<str:totalProductCodeCount>/<str:productcode>/<str:serialno>/', update_pickman_flag, name='update_pickman_flag'),

    # path('render-pdf-html/', views.view_first_pdf_html, name='render_pdf_html'),
      path('add_transaction_detail/', views.add_transaction_detail, name='add_transaction_detail'),
      path('insert-picked-man/', PickedManInsertView.as_view(), name='insert-picked-man'),
    #   path('update_Dispatch_Request/<int:id>/<int:qty>/', UpdateDispatchQtyView.as_view()),
      path('update-truck-picked_Scanned/', UpdateTruckAndPickedManWhileScanView.as_view(), name='update-truck-picked_Scanned'),

     path('Rport_Undelivery_data/', UndeliveredDataViewSet.as_view({'get': 'list'}), name='Rport_Undelivery_data'),


     path('Return_Dispatch_Report_Data/', Return_Dispatch_Report_view.as_view({'get': 'list'}), name='Return_Dispatch_Report_data'),
    path('Rport_Create_dispatch/', InvoiceReports_CreateDispatch.as_view({'get': 'list'}), name='Rport_Create_dispatch'),
    
     path('InterORGReportView/', InterORGReportView.as_view({'get': 'list'}), name='InterORGReportView'),

     
     path('InterORG_Shipment_transferdView/', InterORG_Shipment_transferdView.as_view({'get': 'list'}), name='InterORG_Shipment_transferdView'),
     
     path('InterORGReportCompletedView/', InterORGReportCompletedView.as_view({'get': 'list'}), name='InterORGReportCompletedView'),
   path('Report-grouped-truck-scan-details/', GroupedTruckScanDetailsView.as_view(), name='grouped-truck-scan-details'),

    path('get-salesman-name/', GetSalesmanNameView.as_view(), name='get-salesman-name'),
    path('update-truck-scan/', update_truck_scan_details),
    path('salesrep/<str:salesrep_id>/', views.get_salesrep_details, name='salesrep-details'),
    path('Get_sales_Supervisor_access/<str:salesrep_id>/<str:supervisorno>/', views.Get_sales_Supervisor_access, name='Get_sales_Supervisor_access'),
     path('picked_and_truck_count_view/', picked_and_truck_count_view, name='picked_count'),
    path('test-cors/', views.test_cors),
    path('CreateDispatchWithTruckScanView/', CreateDispatchWithTruckScanView.as_view(), name='CreateDispatchWithTruckScanView'),
    path('GetUndeliveredDataColumnNameview/<str:status>/', GetUndeliveredDataColumnNameview.as_view(), name='GetUndeliveredDataColumnNameview'),
  path("get-salesmanNo_List/", GetSalesmanByCustomer.as_view(), name="get-salesman"),
  path('update-scan-status/<str:req_no>/<str:pick_id>/<str:customer_no>/<str:customer_site>/<str:new_status>/', 
         UpdateScanStatusView.as_view(), name='update_scan_status'),

           path('update-scan-status_Loadman_Scan_Skip/<str:req_no>/<str:pick_id>/<str:customer_no>/<str:customer_site>/<str:new_status>/', 
         UpdateScanStatus_Direct_LoadmanView.as_view(), name='update-scan-status_Loadman_Scan_Skip'),
    path('GetShipmentTableHeadersView/', GetShipmentTableHeadersView.as_view(), name='GetShipmentTableHeadersView'),
    re_path(r'^GetUndeliveredData_columnName_valuesView/(?P<salesmanno>.*)/(?P<status>\w+)/(?P<columnname>\w+)/$', 
            GetUndeliveredData_columnName_valuesView.as_view(), 
            name='GetUndeliveredData_columnName_valuesView'),         
    path('ReqId_generate-token/', GenerateTokenForReqnoView.as_view(), name='ReqId_generate-token'),
    path('Pick_generate-token/', GenerateTokenForPickidView.as_view(), name='Pick_generate-token'),
    path('Quick_Bill_generate-token/', GenerateTokenForQuickBillidView.as_view(), name='Quick_Bill_generate-token'),
    path('Delivery_generate-token/', GenerateTokenForDeliveryIdView.as_view(), name='Delivery_generate-token'),
    path('Return_generate-token/', GenerateTokenForReturnIdView.as_view(), name='Return_generate-token'),
    path("Deliver_Id_Generate/", views.create_deliverid_code, name="generate_delivery_id"),
    path("Shipment_Id_Generate/", views.create_shipment_id, name="generate_shipment_id"),
    path('Shipment_generate-token/', GenerateTokenForShipmentIdView.as_view(), name='SHipment_generate-token'),
    
    path('Invoice_Return_id_generate-token/', GenerateTokenForInvoiceReturnIdView.as_view(), name='Invoice_Return_id_generate-token'),
    path('update-qty/<str:udel_id>/<str:qty>/', UpdateView.as_view(), name='update-qty'),
    path('subractUpdate-qty/<str:udel_id>/<str:qty>/', subractUpdateView.as_view(), name='subractUpdate-qty'),
    
    path('get_department_details/<str:empno>/', get_department_details, name='get_department_details'),
    path('New_Updated_get_submenu_list/<str:dep_role_id>/<str:empno>/', get_submenu_list, name='get_submenu_list'),
    path('New_Updated_get_submenu_depid_list/<str:dep_role_id>/<str:empno>/', get_submenu_depid_list, name='get_submenu_depid_list'),
    path('get_SearchEmployee_data/<str:empid>/', get_SearchEmployee_data, name='get_SearchEmployee_data'),
    path('update_createdispatch_qty/', update_createdispatch_qty, name='update_createdispatch_qty'),
      path('login-connect/', login_connect_table_view, name='login_connect_table'),
    re_path(r'^update_employee/(?P<emp_id>\d+)/?$', update_employee, name='update_employee'),

     path('Generate_dispatch_print/<str:dispatch_id>/', views.Generate_dispatch_print, name='Generate_dispatch_print'),
     path('Generate_picking_print/<str:pickid>/', views.Generate_picking_print, name='Generate_picking_print'),
     path('Generate_dispatch_details_print/<str:dispatch_id>/', views.Generate_dispatch_details_print, name='Generate_dispatch_details_print'),
     path('Generate_Shipment_dispatch_print/<str:shipment_id>/',views.Generate_Shipment_dispatch_print,name='Generate_Shipment_dispatch_print'),
     path("Return_invoice_print/", views.Return_invoice_print, name="Return_invoice_print"),

  path(
        'InvoiceReturn_Details/',
        InvoiceReturnSummaryView.as_view(),
        name='InvoiceReturnSummary'
    ),

      path(
        'Exported_InvoiceReturn_Details/',
        Exported_InvoiceReturnSummaryView.as_view(),
        name='InvoiceReturnSummary'
    ),


path(
        'invoice-return-details/<str:invoice_return_id>/',
        FilterInvoiceReturnDetails.as_view(),
        name='invoice-return-details'
    ),

    path('get-shipment-by-warehouse/', get_shipment_by_warehouse),
    path('get_shipment_by_receviedwarehouse/', get_shipment_by_receviedwarehouse),
    path('get_shipment_by_shipment_numwise_receviedwarehouse/', get_shipment_by_shipment_numwise_receviedwarehouse),
    
    path('update_active_status_by_shipment_id/', update_active_status_by_shipment_id),
    # path('filterstagingdispatch_requests/', views.dispatch_request_list, name='dispatch_request_list'),
    path('filteredPendingdispatch_request_list/', views.filtered_pending_dispatch_request_list, name='filteredPendingdispatch_request_list'),
  path('filteredCompletedDispatch_request_list/', views.filtered_Completed_dispatch_request_list, name='filtered_Completed_dispatch_request_list'),
 
    path('filteredpendingpickman/<str:reqno>/<str:pending>/', views.GetPickmanDetails_PendingView.as_view({'get': 'list'}), name='filteredpickman'),
  
     path('filteredCompletedpickman/<str:reqno>/', views.GetPickmanDetails_CompletedView.as_view({'get': 'list'}), name='GetPickmanDetails_CompletedView'),
  
    path(
        'filteredfinishedpickman/<str:reqno>/<str:cusno>/<str:cussite>',
        views.GetPickmanDetails_FinishedView.as_view({'get': 'list'}),
        name='filteredfinishedpickman'
    ),
    path('Send_pdf_to_email/', views.send_mail_with_pdf),
     path('get-employee-address/<str:empno>/', views.get_employee_address, name='get_employee_address'),

   path('WMS_SoftwareVersionView/<str:softwarename>/', WMS_SoftwareVersionView.as_view({'get': 'list'}), name='WMS_SoftwareVersionView'),
    path('Get_playStore_warning/', GetPlayStoreWarningMsgView.as_view(), name='Get_playStore_warning'),

   path('add_transaction_detail/', views.add_transaction_detail, name='add_transaction_detail'),

    path('upload/', MinioUploadView.as_view(), name='minio-upload'),


    path('get_salesmen/<str:supervisor_no>/', views.get_salesmen_by_supervisor, name='get_salesmen'),

    path('get_unassigned_supervisors/', views.get_unassigned_supervisors),
    path('get_salesmen_excluding_negative3/<str:supervisor_no>/', views.get_salesmen_excluding_negative3),

    path('add_supervisor_access/', add_supervisor_access),
    

  path('update-dispatch/', update_dispatch_request),  # only one URL, no params
  path('insert-picked-man_assign_data/', InsertPickedManAssignData.as_view(), name='insert-picked-man'),
  path('Newinsert-picked-man_assign_data/', NewInsertPickedManAssignData.as_view(), name='insert-picked-man'),
  path('insert-save_truck_scan_data/', InsertSaved_Truck_scan_AssignData.as_view(), name='insert-save_truck_scan'),
         path('update-role/', update_emp_role, name='update_emp_role'),
     path('update_reassign_status/<str:return_dispatch_id>/', update_reassign_status, name='update_reassign_status'),
    path('get-salesman-data/', views.get_salesman_data, name='get_salesman_data'),
    path('GET_Shipment_Interorg/<str:transfer_type>/<str:shipment_header_id>/', views.Get_interorg_data_Shipment, name='Get_interorg_data_Shipment'),
    path('update_Phy_quantity_Shipped_interOrg/', views.update_Phy_quantity_Shipped_interOrg, name='update_Phy_quantity_Shipped_interOrg'),
        path('update_Phy_quantity_Recevied_interOrg/', views.update_Phy_quantity_Recevied_interOrg, name='update_Phy_quantity_Recevied_interOrg'),
    path('Check_InvoiceStatus_CancelInvoice/<str:customerno>/<str:customersiteid>/<str:invoiceno>/', views.Check_InvoiceStatus_CancelInvoice, name='check_invoice_status'),
    path('Update_flag_status_Underlivered/', views.Update_flag_status_Underlivered, name='Update_flag_status_Underlivered'),
    path('Update_Return_Invoice_Undelivered/', Update_Return_Invoice_Undelivered, name='Update_Return_Invoice_Undelivered'),
    path('insert_dispatch/<str:dispatch_id>/<str:undel_id>/<str:qty>/', views.insert_dispatch_data),

    path(
        'insert_Inter_ORG_data//<str:shipment_line_id>/<str:qty>/',
        insert_Inter_ORG_PHY_Shipped_data,
        name='insert_with_id'
    ),
    path('insert_Inter_ORG_PHY_Recevied_data/<str:shipment_id>/<str:shipment_line_id>/<str:qty>/', views.insert_Inter_ORG_PHY_Recevied_data),
    path('Update_assign_Pickman/', AssignPickmanView.as_view(), name='Update_assign_Pickman'),

path('combined_dispatch_raw/', dispatch_progress_raw_view),
path('combined_dispatch_Invoice_details_raw/', dispatch_progress_Invoice_details_raw_view),
path('combined_dispatch_Oracle_Cancel_raw/', dispatch_progress_Oracle_Cancel_view),
  path('update_Oracle_dispatch_flag/<str:reqno>/', update_Oracle_dispatch_flag),
  
  path('Reverse_update_Oracle_dispatch_flag/<str:reqno>/', Reverse_update_Oracle_dispatch_flag),

   # For Inbound Concept url 
   path('get_pending_po/', get_pending_po, name='get_pending_po'),
    path('save_shipment/', SaveShipmentView.as_view()),  # ✅ fix here
    path('generate_docno/', DocNoView.as_view(), name='generate-docno'),
    path('save_container_Info/', SaveContainerInfoView.as_view()),  # ✅ fix here
    path('save_product_info/', SaveProductInfoView.as_view()),  # ✅ fix here
    path('save_expense_details/', SaveExpenseView.as_view()),  # ✅ fix here
    path('get_expense_cat/', get_expense_cat,name='get_expense_cat'),
    path('get_names_by_cat/<str:cat>/', get_names_by_expense_cat,name='get_names_by_cat'),
    path('DocNo_generate-token/', GenerateDocNoView.as_view(), name='DocNo_generate-token'),
    path('check-duplicate/', check_product_serial_duplicate),
    path('check_serial_and_fetch_data/', check_serial_and_fetch_data),

  #  For Dashboard - Salesman
 
    path('get_pending_invoice/<str:salesman_name>/', getPendingInvoice_for_salesman,name='get_pending_invoice'),
    path('get_completed_dispatches/<str:salesman_name>/', getcompleted_dispatches_for_salesman,name='get_completed_dispatches'),
    path('get_on_progress_dispatch/<str:salesman_name>/', get_On_Progress_dispatches_for_salesman,name='get_on_progress_dispatch'),
    path('get_undelivered_customer_count/<str:salesman_name>/', undelivered_customer_count,name='get_undelivered_customer_count'),
 
# For Dashboard - Manager
 
    path('get_dispatch_warehousecount/<str:warehouse_name>/', warehouse_dispatch_count,name='get_undelivered_customer_count'),
    path('get_Pending_pick/<str:physical_warehouse>/', FilteredPendingDispatchRequestCount.as_view()),
    path('LivestageCountView/<str:warehouse_name>/', get_LivestageCountView_by_warehouse, name='get_LivestageCountView_by_warehouse'),
    path('get_disreq_count/<str:warehouse_name>/', get_Dispatch_RequestCount_warehouse,name='get_disreq_count'),
    path('get_delivered_count/<str:warehouse_name>/', delivered_count,name='get_delivered_count'),
    path('get_InterORG_count/', get_InterORG_count,name='get_InterORG_count'),
    path('get_ReturnInvoice_count/<str:warehouse_name>/', ReturnInvoice_count,name='get_ReturnInvoice_count'),
 
    # For Dashboard - Pickman
    path('get_Pending_pickman_count/<str:pickman_name>/', FilteredPendingPickCount.as_view()),
    path('get_pickComplete_count/<str:pickman_name>/', PickCompleted_Count.as_view()),
    path('get_stageReturn_count/<str:pickman_name>/', StageReturn_Count.as_view()),

    # For Dashboard Chart
    path('weekly_dispatches/<str:salesman_name>/', get_weekly_dispatches_for_salesman,name='weekly_dispatches'),
    path('weekly_delivered/<str:warehouse_name>/', get_weekly_delivered_count,name='weekly_delivered'),
    path('weekly_picked/<str:pickman_name>/', get_weekly_picked_count,name='weekly_picked'),
    path("save-invoice-return/", SaveInvoiceReturnHistory.as_view(), name="save_invoice_return"),

    # Project urls 
# path('CustomerNamelist/<str:salesmanno>/', CustomerNamelistView, name='CustomerNamelist'),
   
    path('PendingQtyView/<str:customer_no>/<str:customer_site_id>/', PendingQtyView.as_view(), name='pending_qty'),

   
    path('CustomerNamelist/<str:salesmanno>/', CustomerNamelistView.as_view(), name='CustomerNamelist'),
    path('Invocie_Return_CustomerNamelist/<str:warehousename>/', Invocie_Return_CustomerNamelistView.as_view(), name='Invocie_Return_CustomerNamelistView'),
    path('CustomerSiteIDList/<str:salesmanno>/<str:custno>/', CustomerSiteIDListView.as_view(), name='CustomerSiteIDList'),
    path('Invoice_Return_CustomerSiteIDList/<str:warehousename>/<str:custno>/', Invoice_Return_CustomerSiteIDListView.as_view(), name='Invoice_Return_CustomerSiteIDListView'),

    path('Create_DispatchReqno/', ReqnoView.as_view(), name='Create_DispatchReqno'),
    
    path('Generate_Picking_PickId/', Pickid_View.as_view(), name='Generate_Picking_PickId'),
    path('Delivery_Id_View/', DeliveryID_view.as_view(), name='Delivery_Id_View'),
    path('Return_dispatchNo/', ReturnID_view.as_view(), name='ReturnID_view'),
    
    path('Shipment_dispatchNo/', ShipmentID_view.as_view(), name='ShipmentID_view'),
    
    
    path('Invoice_ReturnID_Lastid/', Invoice_ReturnID_view.as_view(), name='Invoice_ReturnID_view'),

    path('balance_dispatch/', Filtered_balanceDispatchView.as_view({'get': 'filter_dispatch'}), name='filter-dispatch'),
    path('invoice/<str:salesmanno>/<str:cusnumber>/<str:cussiteid>/', SalesInvoiceDetailsView.as_view(), name='SalesInvoiceDetailsView'),
    path('top_product/', TopProductsViewSet.as_view({'get': 'list'}), name='top_products_view'),

 path(
        'filtered_dispatchrequest/<str:REQ_ID>/<str:warehouse_name>/',
        Filtered_dispatchRequestView.as_view({'get': 'list'}),
        name='filtered_dispatch_request'
    ),
 path(
        'Filtered_Returndispatch/<str:RETURN_DIS_ID>/<str:warehouse_name>/',
        Filtered_ReturndispatchView.as_view({'get': 'list'}),
        name='Filtered_ReturndispatchView'    ),
         
path(
        'Filtered_InterORGReportView/<str:shipment_id>/',
        Filtered_InterORGReportView.as_view({'get': 'list'}),
        name='Filtered_InterORGReportView'
    ),
    path('highest-pick/', HighestPickIDView.as_view(), name='highest-pick'),                             
    path('Filtered_Pickscan/<str:REQ_ID>/<str:PICK_ID>', 
         Filtered_PickscanView.as_view({'get': 'list'}), 
         name='Filtered_Pickscan'),


    path('ViewPickiddetails/<str:PICK_ID>', 
         ViewPickiddetailsView.as_view({'get': 'list'}), 
         name='ViewPickiddetails'),


    re_path(
        r'^filteredProductcodeGetView‡(?P<itemcode>[^‡]*)‡(?P<DESCRIPTION>[^‡]*)‡$',
        filteredProductcodeGetView.as_view({'get': 'list'}),
        name='ProductcodeGetView'
    ),
    
#     path('UpdateUser_member_detailsView/<str:role>/', 
#          User_member_UniqueIdView.as_view({'get': 'list'}), 
#          name='Filtered_Pickscan'),
     path('Pickman_Productcode/<str:reqno>/<str:productcode>/<str:serialno>/', 
         Pickman_Productcode.as_view({'get': 'list'}), 
         name='Pickman_Productcode'),

   path('Truck_scan_DispatchNo/', Truck_scan_DispatchNoView.as_view(), name='Truck_scan_DispatchNo'),
 
  
   path(
        'Filtered_livestagereports/',
        FilteredLivestageView.as_view({'get': 'list'}),
        name='Filtered_livestagereports'
    ),
    path(
        'CompletedDispatchFilteredLivestageView/<str:reqno>/',
        CompletedDispatchFilteredLivestageView.as_view({'get': 'list'}),
        name='CompletedDispatchFilteredLivestageView'
    ),

    path('Combined_livestage_report/', CombinedLivestageReportView.as_view(), name='combined-livestage-report'),

    path('NewCombined_livestage_report/', NewCombinedLivestageReportView.as_view(), name='combined-livestage-report'),
    path('Quick_Bill_Combined_livestage_report/', Quick_Bill_CombinedLivestageReportView.as_view(), name='combined-livestage-report'),
   path(
    'Livestagebuttonstaus/<str:reqno>/<str:pickno>/<str:Customer_no>/<int:count>/<str:cussite>/',
    LivestagebuttonstausView.as_view({'get': 'retrieve'}),
    name='dispatch-status'
   ),
   path(
        'filteredToGetGenerateDispatchView/<str:req_no>/<str:Customer_no>/<str:Customer_Site>/',
        ToGetGenerateDispatchView.as_view({'get': 'retrieve'}),
        name='ToGetGenerateDispatchView'
    ),
    path(
        'filtered_Truck/',
        filtered_TruckView.as_view({'get': 'list'}),
        name='filtered_Truck'
    ),
    

    path('dispatch-details/<str:REQ_ID>/',
         DispatchDetailsView.as_view({'get': 'list'}),
         name='dispatch-details'),

    path('filtedshippingproductdetails/<str:dispatchid>/', filtedshippingproductdetailsView.as_view({'get': 'list'}), name='filtered_shipping_products'),
    path('compare-scan/<str:req_id>/<str:customername>/<str:customersite>/', CompareScanDat_for_bypassView.as_view(), name='compare-scan'),
    path('compare-scan-noproductcode/<str:req_id>/<str:customername>/<str:customersite>/', CompareScanDat_for_noproductcodeView.as_view(), name='compare-scan-noproductcode'),
    path('compare-scan-noserialno/<str:req_id>/<str:customername>/<str:customersite>/', CompareScanDat_for_noserialnoView.as_view(), name='compare-scan-noserialno'),
       path('Deliver_noserialno_bypassesView/<str:dispatch_id>/', Deliver_noserialno_bypassesView.as_view(), name='Deliver_noserialno_bypassesView'),
   
    path('Filtered_ReturnDispatch/<str:dispatch_id>/', Filtered_ReturnDispatchView.as_view({'get': 'list'}), name='Filtered_ReturnDispatch'),
    path('Truck_ProductCodedetails/<str:dispatchid>/<str:productcode>/<str:serialno>/', 
         Truck_Productcode.as_view({'get': 'list'}), 
         name='Truck_Productcode'),
    re_path(
        r'^filtereddispatchrequestgetreturnupdateid‡(?P<reqid>[^‡]*)‡(?P<cusno>[^‡]*)‡(?P<cussite>[^‡]*)‡(?P<itemcode>[^‡]*)‡$',
        FilteredDispatchRequestView.as_view({'get': 'list'}),
        name='filtereddispatchrequestgetreturnupdateid'
    ),
    # path(
    #     'filtereddispatchrequestgetreturnupdateid/<str:reqid>/<str:cusno>/<str:cussite>/<str:itemcode>/',
    #     FilteredDispatchRequestView.as_view({'get': 'list'}),
    #     name='filtereddispatchrequestgetreturnupdateid'
    # ),
  
    re_path(
        r'^GetidCreateDispatchView‡(?P<reqid>[^‡]*)‡(?P<cusno>[^‡]*)‡(?P<cussite>[^‡]*)‡(?P<invoiceno>[^‡]*)‡(?P<itemcode>[^‡]*)‡$',
        GetIdCreateDispatchView.as_view({'get': 'list'}),
        name='GetidCreateDispatchView'
    ),
    # path('GetidCreateDispatchView/<str:reqid>/<str:cusno>/<str:cussite>/<str:invoiceno>/<str:itemcode>/',GetIdCreateDispatchView.as_view({'get': 'list'}),name='GetIdCreateDispatchView'),
    path('FilteredCreateDispatchView/<str:reqid>/<str:cusno>/<str:cussite>/',FilteredCreateDispatchView.as_view({'get': 'list'}),name='FilteredCreateDispatchView'),
    path('GetFlagRcountcreatedispatchView/<str:reqid>/<str:cusno>/<str:cussite>/',GetFlagRcountcreatedispatchView.as_view({'get': 'list'}),name='GetFlagRcountcreatedispatchView'), 
    path(
        'UpdateCreateDispatchRequestView/',
        UpdateCreateDispatchRequestView.as_view(),
        name='update_dispatch'
    ), 
      
        path('CommericialDispatch/<str:commericialno>/', CommericialDispatch.as_view({'get': 'list'}), name='CommericialDispatch'),
    path(
        'filteredreturnView/<str:reqid>/<str:cusno>/<str:cussite>/',
        filteredreturnView.as_view({'get': 'list'}),
        name='filteredreturnView'
    ),
     path(
        'updatedfilteredProductcodeGetView/<str:itemcode>/',
        updatedfilteredProductcodeGetView.as_view({'get': 'list','put': 'update'}),  # Use 'list' action to filter and return data
        name='updatedfilteredProductcodeGetView'
    ),
      path(
        'update-product-code/<str:itemcode>/<str:productcode>/',
        UpdateProductCodeView.as_view({'get': 'update_product_code', 'put': 'update_product_code'}),
        name='update_product_code'
    ),
     path(
        'Findstatusforstagereturn/<str:reqid>/<str:productcode>/<str:serialno>/',
        Findstatusforstagereturn.as_view(),
        name='Findstatusforstagereturn'
    ),
    path('GetFilteredTruckDetailsView/<str:reqid>/<str:cusno>/<str:cussite>/<str:itemcode>/',GetFilteredTruckDetailsView.as_view({'get': 'list'}),name='GetFilteredTruckDetailsView'),
  
    # path('highest-pick/', HighestPickIDView.as_view({'get': 'list'}), name='highest-pick'),
    # path('Amc/<str:cusid>/', Amc_tblView.as_view({'get': 'list'}), name='amc-cusid'),
    # path('undelivered-data/', FetchUndeliveredData),

    # Vesrion Ceck Url
    path("version/", APIVersionView.as_view(), name="api-version"),


    # # Monitoring URL
    # path('monitor/', views.log_view, name='log_monitor'),
    # path('monitor/delete-filtered/', views.delete_filtered_logs, name='delete_filtered_logs'),
    # path('monitor/delete-all/', views.delete_all_logs, name='delete_all_logs'),
    # path('logs/<int:pk>/', views.log_detail, name='log_detail'),

     path('save_dispatch_request/', views.save_dispatch_request, name='save_dispatch_request'),     
    path("truck_scan_view/", views.truck_scan_view, name="truck_scan_view"),


    path("insert_delivery_header/", views.insert_delivery_header, name="insert_delivery_header"),
    path("Get-Pickman_dispatch-request/", GetPickmanDetailsView.as_view(), name="Get-Pickman_dispatch-request"),

    path("save-login-details/", SaveLoginDetailsView.as_view(), name="save_login"),

    path("Show_button_truck_details/", views.Show_button_truck_details, name="Show_button_truck_details"),

    path("Show_button_truck_details_Loadman_Skip_Scan/", views.Show_button_truck_details_Loadman_Skip_Scan, name="Show_button_truck_details_Loadman_Skip_Scan"),
    path("check_Insert_update_Status/", check_Insert_update_Status_view, name="check_Insert_update_Status"),

    path("insert-picked-to-truck/", InsertPickedDataToTruckView.as_view(), name="insert-picked-to-truck"),
    path("add_employee_access/", views.employee_access_view, name="employee_access"),

    path("Get_employee_access_type/<str:employee_id>/", views.get_employee_access_view, name="get_employee_access"),

    path("Get-whr-superusers/<int:org_id>/", get_whr_superuser_list),
    
    path("Update_employee_access/", update_employee_access),
    path('wms_formname_check/', views.wms_formname_check_view, name='wms_formname_check'),
    path("update-dispatch-balance/", update_dispatch_balance, name="update-dispatch-balance"),

    path('add-shipment-deatils/', add_shipment_dispatch_update, name='add_shipment_dispatch_update'),
    path('quick-bill-visibility/', quick_bill_visibility, name='quick_bill_visibility'),
path(
        'Quick_Bill_Process-Get-whr-superuser-list/',
        Quick_Bill_acess_Get_whr_superuser_list,
        name='Quick_Bill_Process-Get-whr-superuser-list'
    ),

        path('whr_user_management_internal_damage/', whr_user_management_internal_damage, name='whr_user_management_internal_damage'),


    path(
        'Update-quick-bill-access/',
        update_user_access,
        name='update_quick_bill_access'
    ),

     path(
        'get-quick-bill-enable/',
        get_user_access_status_by_employeeid,
        name='get_quick_bill_enable_status'
    ),

 path(
        'Update-Admin-quick-bill-visible/',
        update_admin_quick_bill_visible,
        name='update_quick_bill_visible'
    ),

       path(
        'get-Admin-quick-bill-visible/',
        get_Admin_quick_bill_visible,
        name='get_quick_bill_visible'
    ),

       path("dispatch-item-details/", dispatch_item_details_view,name="dispatch-item-details"),

         path(
        "delivery-confirmation_insert/",
        insert_delivery_confirmation,
        name="insert_delivery-confirmation"
    ),

      path(
        "Get_Dispatch_unconfirmed/",
        unconfirmed_dispatch_list_view,
        name="get_dispatch_unconfirmed"
    ),

path('Get_dispatch_confirmation_details/', views.get_dispatch_details, name='Get_dispatch_confirmation_details'),
path('undelivered_Request_invoice_details/', views.get_undelivered_invoice_details, name='undelivered_Request_invoice_details'),

##################################### Automation Create dispatch Request APIs ########################################

path('get_next_arg_no/', views.get_next_arg_no, name='get_next_arg_no'),
path('undelivered_consolidated_details/', views.get_consolidated_undelivered_details, name='get_consolidated_undelivered_details'),
path('create_dispatch_for_Auto_Generate/', views.create_dispatch_for_Auto_Generate, name='create_dispatch_for_Auto_Generate'),


 path(
        "MultiInternal_Damage_ImageUploadView/",
        MultiInternal_Damage_ImageUploadView.as_view(),
        name="MultiInternal_Damage_ImageUploadView"
    ),


    
#--------------------------------------------------------------------------------------------------------------------
# get save truck scan detials fro loadman skip scan process
#--------------------------------------------------------------------------------------------------------------------

   path(
        'get_save_truck_scan_details_by_req_pick/',
        save_truck_details_by_req_pick,
        name='get_save_truck_scan_details_by_req_pick'
    ),

#-------------------------------------------------------------------------------------------------------
#update transportor charge, loading charge, misc charges
#-------------------------------------------------------------------------------------------------------


        path('update-dispatch-charges/', views.update_dispatch_charges),


path('get_unique_truck_scan_details_Report/', views.get_unique_truck_scan_details_Report, name='get_unique_truck_scan_details_Report'),

path('dispatch-fulfilled-records/', finance_tema_View.dispatch_fulfilled_records_view, name='dispatch_fulfilled_records_view'),
path('dispatch-fulfilled-invoice-records/', finance_tema_View.dispatch_fulfilled_invoice_details_view, name='dispatch_fulfilled_invoice_details_view'),

    path('get-customer-name/', views.get_customer_name, name='get_customer_name'),

]
