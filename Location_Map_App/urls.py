
from django.urls import path, include,re_path

from ALJE_APP import views

from .views import location_mapping_views,internal_transfer_view, Internal_damage_view,session_view

from Location_Map_App.views.Internal_damage_view import *
from . import models
from rest_framework import routers
from Location_Map_App.views.location_mapping_views import *
from django.conf import settings
from django.conf.urls.static import static


router = routers.DefaultRouter()

urlpatterns = [
    path("", include(router.urls)),    

###################################################### Location Mapping #########################################################################

    path('Get_WHR_ID_Status/<str:WHR_Code>/', location_mapping_views.Get_whr__ID_status, name='Get_whr__ID_status'),
    path("Get_Zone_Id_Status/<str:WHR_Code>/<str:WHR_ID>/", Get_Zone_Id_Status, name="zone_id"),
    path("Get_Bin_ID_Status/<str:WHR_Code>/<str:WHR_ID>/<str:Zone_ID>/", location_mapping_views.Get_Bin_Id_Status,name='Get_Bin_Id_Status'),
    path('Get_Location_By_Barcode/<str:Physical_WHR>/<str:WHR_Barcode>/', get_location_by_barcode),
    path('Get_Location_By_Only_Barcode/<str:WHR_Barcode>/', get_location_by_only_barcode),
    path("Get_Item_Details/", location_mapping_views.get_item_details),
    path(
        'Update_LOC_Measurement/<str:physical_whr>/<str:WhR_ID>/<str:whr_measurement>/',
        update_loc_measurement
    ),
    path("Get_WHR_Code/", get_whr_code, name="WHR_Code"),
    path("Get_WHR_ID/<str:WHR_Code>/", get_whr_id, name="get_whr_id"),
    
    path("Zone_Id/<str:WHR_Code>/<str:WHR_ID>/", get_zone_id, name="zone_id"),
    path("Get_Bin_ID/<str:WHR_Code>/<str:WHR_ID>/<str:Zone_ID>/", get_bin_id),
    path("Post_WHR/<str:Physical_WHR>/<str:WHR_Code>/", location_mapping_views.Post_WHR),
    path(
    "Post_WHR_Zone/<str:Physical_WHR>/<str:WHR_Code>/<str:WHR_ID>/",
    location_mapping_views.Post_WHR_Zone),
    path(
    "Post_WHR_Bin/<str:Physical_WHR>/<str:WHR_Code>/<str:WHR_ID>/<str:Zone_ID>/",
    location_mapping_views.Post_WHR_Bin),
    path(
        "Update_Deactivate_WHR/<str:Physical_WHR>/<str:WHR_Code>/<str:WHR_ID>/<str:Flag1>/",
        location_mapping_views.Deactivate_WHR,
        name="Deactivate_WHR"
    ),
    path(
    "Update_Deactivate_Zone/<str:Physical_WHR>/<str:WHR_Code>/<str:WHR_ID>/<str:Zone_ID>/<str:Flage1>/",
    location_mapping_views.Deactivate_WHR_Zone,
    name="Deactivate_WHR"
   ),
    path(
        "Update_Deactivate_Bin/<str:Physical_WHR>/<str:WHR_Code>/<str:WHR_ID>/<str:Zone_ID>/<str:Bin_ID>/<str:Flage1>/",
        location_mapping_views.Deactivate_WHR_Statu,
        name="Deactivate_WHR"
    ),
    path(
    'Get_WHR_Measurement/<str:physical_whr>/<str:whr_id>/',
    get_whr_measurement),
    path('Get-Location-Measurement/', location_mapping_views.get_location_measurement_sql),

   path('insert-whr-stock/', insert_whr_stock),
    path('UniqId/', GenerateTokenFormMNG_UNIQIDView.as_view(), name='SHipment_generate-token'),
    path('Get_Stock_Manager/', get_whr_summary, name='Shipment_generate_token'),
    path("Get_stock_Summery/<str:uniq_id>/", whr_stock_summary),
    path("insert-details/", insert_whr_stock_details),
    path("get-serial-numbers/", get_serial_numbers, name="get_serial_numbers"),
    path('update-serial-numbers/', update_serial_numbers),
    path("Item_Details_WHRLOC/<str:Status>/<str:Value>/", location_mapping_views.item_details_whrloc, name='item_details_whrloc'),
    path('get-WHRLOC-item-details/', get_WHRLOC_item_details, name='get-WHRLOC-item-details'),


 ##################################################### Location Mapping for pickma scan  ####################################################################
 path(
        "item-available-locations/",
        item_available_location_list,
        name="item-available-locations"
    ),

   path(
        "update-stock-adispatch/",
        UpdateStockDispatchView.as_view(),
        name="update-stock-dispatch"
    ),


    path('update_stock_serial_wise/', location_mapping_views.update_stock_serial_wise, name='update_stock_serial_wise'),
    path("update-stock-details/", update_stock_details, name="update_stock_details"),
    #-----------------------------------------------------------------------------------------------------
                                        # Item Measurement Units #
    #-----------------------------------------------------------------------------------------------------
    path('insert-item-measurement/', location_mapping_views.insert_item_measurement, name='insert_item_measurement'),
    path('get-item-measurement/', location_mapping_views.get_item_measurement, name='get_item_measurement'),
    path('update-item-measurement/', location_mapping_views.update_item_measurement, name='update_item_measurement'),

 #-----------------------------------------------------------------------------------------------------
                                        # Internal Damage details #
    #-----------------------------------------------------------------------------------------------------
   
path(
        'check-item-location-query/',
        check_item_location_status_query,
        name='check_item_location_query'
    ),

path('insert_internal_damage_details/', Internal_damage_view.insert_internal_damage_details, name='insert_internal_damage_details'),

path('get_internal_damage_details/', Internal_damage_view.get_internal_damage_details, name='get_internal_damage_details'),

path('update_internal_damage_details/', Internal_damage_view.update_internal_damage_status, name='update_internal_damage_details'),


#-----------------------------------------------------------------------------------------------------------
                                        # Seesion Limit 
#-----------------------------------------------------------------------------------------------------------

  path('login/', session_view.login_view, name='login'),
    path('signup/', session_view.signup_view, name='signup'),
    path('dashboard/', session_view.dashboard_view, name='dashboard'),
    path('logout/', session_view.logout_view, name='logout'),
    path('config/', session_view.update_config_view, name='config'),
   
    # API endpoints for Tab Handling
    path('api/check-tab-session/', session_view.check_tab_session, name='check_tab_session'),
    path('api/activate-tab-session/', session_view.activate_tab_session, name='activate_tab_session'),
    path('api/session-heartbeat/', session_view.session_heartbeat, name='session_heartbeat'),
   
    # Flutter APIs
    path('api/login/', session_view.api_login, name='api_login'),
    path('api/signup/', session_view.api_signup, name='api_signup'),
    path('api/logout/', session_view.api_logout, name='api_logout'),

    path("Insert_session_login/", session_view.login_dynamic),
    path('idle-timeout/', session_view.get_idle_timeout_seconds, name='idle-timeout'),

    #-----------------------------------------------------------------------------------------------------------
    # Inbound wise Inter stokc details
    #-----------------------------------------------------------------------------------------------------------

    path('Inbound_wise_insert_whr_stock_serial/', location_mapping_views.Inbound_wise_insert_whr_stock_serial, name='Inbound_wise_insert_whr_stock_serial'),
    path('insert-internal-transfer-details/', internal_transfer_view.insert_internal_transfer_details, name='insert_internal_transfer_details'),
    path('get-internal-transfer-details/', internal_transfer_view.get_internal_transfer_details, name='get_internal_transfer_details'),
    ]

if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
