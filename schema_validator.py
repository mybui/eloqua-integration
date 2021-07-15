from logging_config import setup_logging
import re

logger = setup_logging(__name__)

contact_fields = ["C_Firstname", "C_Lastname", "C_MobilePhone", "C_EmailAddress",
                  "C_Salutation", "C_Country", "C_IM_CRM_Status1", "C_IM_CRM_Contact_ID1",
                  "C_IM_CRM_Institution_ID1", "C_Company", "IM_CRM_GDPR", "C_IM_CRM_Gender1", "C_IM_CRM_Target1",
                  "C_IM_CRM_Function1", "C_IM_CRM_Person_Qualification1", "C_IM_CRM_Person_Tendency1",
                  "C_IM_CRM_KAM_Person1", "IM_CRM_Product_Interest1", "IM_CRM_Segmentation1", "IM_CRM_Person_Type1",
                  "C_Title", "C_Job_Role1", "IM_CRM_Doctor_in_Training1", "C_B2B_Email_Consent1",
                  "C_B2B_Email_Consent_Date1", "C_B2B_Email_Consent_Source1", "C_IM_CRM_User_Name1",
                  "C_IM_CRM_Security_Label1", "C_IM_CRM_Lead_Score1", "C_IM_CRM_Lead_Status1",
                  "C_LastModifiedByExtIntegrateSystem"]

activity_fields = ["Meeting_Type", "Meeting_SubType", "Meeting_ID", "Start_Time", "End_time", "Duration",
                   "Accomplished", "Meeting_Place", "IM_CRM_InstitutionID", "Created_Date", "Modified_Date",
                   "Owner", "Created_By", "Updated_By", "ProductsandCampaigns"]

institution_fields = ["CompanyName", "Address1", "Address2", "City", "Zip_Postal", "IM_CRM_Brick", "Country",
                      "BusPhone", "State_Prov", "IM_CRM_Status", "IM_CRM_Institution_Department",
                      "IM_CRM_Institution_ID", "M_CRM_Institution_Territory", "IM_CRM_Institution_Type",
                      "IM_CRM_Institution_Title", "IM_CRM_Institution_Specialty", "IM_CRM_Category",
                      "IM_CRM_Institution_Level", "IM_CRM_Institution_Segmentation", "IM_CRM_Institution_Area"]

person_activity_fields = ["IM_CRM_Contact_ID", "IM_CRM_Meeting_ID", "IM_CRM_Row_ID"]

person_institution_fields = ["IM_CRM_Contact_ID", "IM_CRM_Institution_ID", "IM_CRM_Row_ID", "IM_CRM_Status"]

email_pattern = "^(\w|\.|\_|\-)+[@](\w|\_|\-|\.)+[.]\w{2,3}$"


def check_valid_email(email):
    if re.search(email_pattern, email):
        return True
    return False


def check_data_schema(data, fields):
    passed_data = None
    if fields == contact_fields:
        passed_data = [i for i in data if len([n for n in i.keys() if n not in fields]) == 0 and
                       i.get("C_IM_CRM_Contact_ID1", "") and
                       i.get("C_IM_CRM_Security_Label1", "") and
                       check_valid_email(i.get("C_EmailAddress", ""))]
    if fields == activity_fields:
        passed_data = [i for i in data if len([n for n in i.keys() if n not in fields]) == 0 and
                       i.get("Meeting_ID", "")]
    if fields == institution_fields:
        passed_data = [i for i in data if len([n for n in i.keys() if n not in fields]) == 0 and
                       i.get("IM_CRM_Institution_ID", "")]
    if fields == person_activity_fields:
        passed_data = [i for i in data if len([n for n in i.keys() if n not in fields]) == 0 and
                       i.get("IM_CRM_Meeting_ID", "") and
                       i.get("IM_CRM_Contact_ID", "")]
    if fields == person_institution_fields:
        passed_data = [i for i in data if len([n for n in i.keys() if n not in fields]) == 0 and
                       i.get("IM_CRM_Institution_ID", "") and
                       i.get("IM_CRM_Contact_ID", "")]
    return passed_data
