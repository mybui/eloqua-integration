from flask import g, current_app
from pymongo import MongoClient

from logging_config import setup_logging

from datetime import datetime, timedelta

logger = setup_logging(__name__)


def get_db():
    if "db" not in g:
        g.db = Database()
    return g.db


class Database:
    def __init__(self):
        self._client = MongoClient()
        self._database = self._client[current_app.config["CLOUD_APP_DB_NAME"]]
        # current collections
        self._contact = self._database["contact"]
        self._activity = self._database["activity"]
        self._institution = self._database["institution"]
        self._personActivity = self._database["personActivity"]
        self._personInstitution = self._database["personInstitution"]
        # past collections
        self._contactPast = self._database["contactPast"]
        self._activityPast = self._database["activityPast"]
        self._institutionPast = self._database["institutionPast"]
        # all activities collection
        self._allActivities = self._database["allActivities"]
        # joined collections
        self._joinedActivity = self._database["joinedActivity"]
        self._joinedInstitution = self._database["joinedInstitution"]

    def insert_data(self, collection, data):
        try:
            if data:
                # current collections
                if collection == "activity":
                    self._activity.insert_many(data)
                    logger.debug("{0} Activity CDO inserted to DB".format(len(data)))
                if collection == "institution":
                    self._institution.insert_many(data)
                    logger.debug("{0} Institution CDO inserted to DB".format(len(data)))
                if collection == "contact":
                    self._contact.insert_many(data)
                    logger.debug("{0} Contact inserted to DB".format(len(data)))
                if collection == "personActivity":
                    self._personActivity.insert_many(data)
                    logger.debug("{0} Person Activity inserted to DB".format(len(data)))
                if collection == "personInstitution":
                    self._personInstitution.insert_many(data)
                    logger.debug("{0} Person Institution inserted to DB".format(len(data)))
                # all activities collection
                if collection == "allActivities":
                    self._allActivities.insert_many(data)
                    logger.debug("{0} Activity inserted to DB".format(len(data)))
                # joined collections
                if collection == "joinedActivity":
                    self._joinedActivity.insert_many(data)
                    logger.debug("{0} Joined Activity CDO inserted to DB".format(len(data)))
                if collection == "joinedInstitution":
                    self._joinedInstitution.insert_many(data)
                    logger.debug("{0} Joined Institution CDO inserted to DB".format(len(data)))
                # past collections
                if collection == "contactPast":
                    self._contactPast.insert_many(data)
                    logger.debug("{0} Contact archived (current data moved to past)".format(len(data)))
                if collection == "activityPast":
                    self._activityPast.insert_many(data)
                    logger.debug("{0} Activity CDO archived (current data moved to past)".format(len(data)))
                if collection == "institutionPast":
                    self._institutionPast.insert_many(data)
                    logger.debug("{0} Institution CDO archived (current data moved to past)".format(len(data)))
                return True
            else:
                logger.debug("Cannot insert empty data to collection {0} in DB".format(collection))
                return False
        except:
            logger.debug("Cannot insert data to collection {0} in DB".format(collection))
            return False

    def get_cdo_data(self, collection, region):
        if collection == "activity":
            # perform left outer join
            # without-merge join
            pipeline = [{"$lookup": {"from": "personActivity", "localField": "Meeting_ID",
                                     "foreignField": "IM_CRM_Meeting_ID", "as": "activityDetail"}},
                        {"$match": {"Meeting_ID": {"$regex": region}}},
                        {"$unwind": {"path": "$activityDetail", "preserveNullAndEmptyArrays": False}}]
            joined_activity = list(self._activity.aggregate(pipeline=pipeline))
            if joined_activity:
                # perform some transformation on join results
                for i in joined_activity:
                    contact_id = i["activityDetail"]["IM_CRM_Contact_ID"]
                    row_id = i["activityDetail"]["IM_CRM_Row_ID"]
                    i["IM_CRM_Contact_ID"] = contact_id
                    i["IM_CRM_Row_ID"] = row_id
                    i.pop("activityDetail", None)
                    i.pop("_id", None)
                # insert joined data to joined collections as temporary staging areas
                self.insert_data(collection="joinedActivity", data=joined_activity)
                # return joined data from joined collection
                # (failed if directly return the joined data from above)
                activities = list(self._joinedActivity.find({}, {"_id": 0}))
                # filter for unique activities
                return list(map(dict, set(tuple(activity.items()) for activity in activities)))
            else:
                logger.debug("No joined Activity CDO for country {0}".format(region.pattern))
                return None
        if collection == "institution":
            pipeline = [{"$lookup": {"from": "personInstitution", "localField": "IM_CRM_Institution_ID",
                                     "foreignField": "IM_CRM_Institution_ID", "as": "institutionDetail"}},
                        {"$match": {"IM_CRM_Institution_ID": {"$regex": region}}},
                        {"$unwind": {"path": "$institutionDetail", "preserveNullAndEmptyArrays": False}}]
            joined_institution = list(self._institution.aggregate(pipeline=pipeline))
            if joined_institution:
                for i in joined_institution:
                    contact_id = i["institutionDetail"]["IM_CRM_Contact_ID"]
                    row_id = i["institutionDetail"]["IM_CRM_Row_ID"]
                    i["IM_CRM_Contact_ID"] = contact_id
                    i["IM_CRM_Row_ID"] = row_id
                    i.pop("institutionDetail", None)
                    i.pop("_id", None)
                self.insert_data(collection="joinedInstitution", data=joined_institution)
                institutions = list(self._joinedInstitution.find({}, {"_id": 0}))
                return list(map(dict, set(tuple(institution.items()) for institution in institutions)))
            logger.debug("No joined Institution CDO for country {0}".format(region.pattern))
            return None

    def get_contact_data(self, region):
        contacts = list(self._contact.find({"C_IM_CRM_Security_Label1": {"$regex": region}}, {"_id": 0}))
        if contacts:
            # filter for unique contacts
            return list(map(dict, set(tuple(contact.items()) for contact in contacts)))
        return None

    def get_all_activities_data(self, type=None):
        if type == "PageView":
            return list(self._allActivities.find({"ActivityType": "PageView"}, {"_id": 0}))
        return list(self._allActivities.find({"ActivityType": {"$ne": "PageView"}}, {"_id": 0}))

    def get_past_data(self, collection, region):
        if collection == "activityPast":
            return list(self._activityPast.find({"Meeting_ID": {"$regex": region}}, {"_id": 0}))
        if collection == "institutionPast":
            return list(self._institutionPast.find({"IM_CRM_Institution_ID": {"$regex": region}}, {"_id": 0}))
        if collection == "contactPast":
            return list(self._contactPast.find({"C_IM_CRM_Security_Label1": {"$regex": region}}, {"_id": 0}))

    def query_all_activities_data(self, activity_date_from, activity_date_to, region):
        if len(self.get_all_activities_data()) != 0:
            if region:
                if activity_date_from and activity_date_to:
                    return list(self._allActivities.find({"$and": [{"ActivityDate": {"$gt": activity_date_from,
                                                                                     "$lt": activity_date_to}},
                                                                   {"C_IM_CRM_Contact_ID1": {"$regex": region}}]},
                                                         {"_id": 0}))
                elif activity_date_from:
                    return list(self._allActivities.find({"$and": [{"ActivityDate": {"$gt": activity_date_from}},
                                                                   {"C_IM_CRM_Contact_ID1": {"$regex": region}}]},
                                                         {"_id": 0}))
                elif activity_date_to:
                    return list(self._allActivities.find({"$and": [{"ActivityDate": {"$lt": activity_date_to}},
                                                                   {"C_IM_CRM_Contact_ID1": {"$regex": region}}]},
                                                         {"_id": 0}))
                else:
                    return list(self._allActivities.find({"C_IM_CRM_Contact_ID1": {"$regex": region}},
                                                         {"_id": 0}))
            else:
                if activity_date_from and activity_date_to:
                    return list(self._allActivities.find({"ActivityDate": {"$gt": activity_date_from,
                                                                           "$lt": activity_date_to}},
                                                         {"_id": 0}))
                elif activity_date_from:
                    return list(self._allActivities.find({"ActivityDate": {"$gt": activity_date_from}},
                                                         {"_id": 0}))
                elif activity_date_to:
                    return list(self._allActivities.find({"ActivityDate": {"$lt": activity_date_to}},
                                                         {"_id": 0}))
                else:
                    return self.get_all_activities_data()
        else:
            logger.debug("No Activity existed")
            return list()

    def query_all_activities_data_with_contact(self, contact_date_from, contact_date_to, region):
        if len(self.get_all_activities_data()) != 0:
            if region:
                if contact_date_from and contact_date_to:
                    return list(self._allActivities.find({"$and": [{"C_DateModified": {"$gt": contact_date_from,
                                                                                       "$lt": contact_date_to}},
                                                                   {"C_IM_CRM_Contact_ID1": {"$regex": region}}]},
                                                         {"_id": 0}))
                elif contact_date_from:
                    return list(self._allActivities.find({"$and": [{"C_DateModified": {"$gt": contact_date_from}},
                                                                   {"C_IM_CRM_Contact_ID1": {"$regex": region}}]},
                                                         {"_id": 0}))
                elif contact_date_to:
                    return list(self._allActivities.find({"$and": [{"C_DateModified": {"$lt": contact_date_to}},
                                                                   {"C_IM_CRM_Contact_ID1": {"$regex": region}}]},
                                                         {"_id": 0}))
                else:
                    current_time = datetime.utcnow()
                    past_time = datetime.utcnow() - timedelta(days=1)
                    contact_date_from_default = str(past_time.date()) + " " + str(past_time.time())
                    contact_date_to_default = str(current_time.date()) + " " + str(current_time.time())
                    return list(self._allActivities.find({"$and": [{"C_DateModified": {"$gt": contact_date_from_default,
                                                                                       "$lt": contact_date_to_default}},
                                                                   {"C_IM_CRM_Contact_ID1": {"$regex": region}}]},
                                                         {"_id": 0}))
            else:
                if contact_date_from and contact_date_to:
                    return list(self._allActivities.find({"C_DateModified": {"$gt": contact_date_from,
                                                                             "$lt": contact_date_to}},
                                                         {"_id": 0}))
                elif contact_date_from:
                    return list(self._allActivities.find({"C_DateModified": {"$gt": contact_date_from}},
                                                         {"_id": 0}))
                elif contact_date_to:
                    return list(self._allActivities.find({"C_DateModified": {"$lt": contact_date_to}},
                                                         {"_id": 0}))
                else:
                    current_time = datetime.utcnow()
                    past_time = datetime.utcnow() - timedelta(days=1)
                    contact_date_from_default = str(past_time.date()) + " " + str(past_time.time())
                    contact_date_to_default = str(current_time.date()) + " " + str(current_time.time())
                    return list(self._allActivities.find({"C_DateModified": {"$gt": contact_date_from_default,
                                                                             "$lt": contact_date_to_default}},
                                                         {"_id": 0}))
        else:
            logger.debug("No Activity existed")
            return list()

    def delete_data(self, collection, region=None):
        try:
            # current collections
            if collection == "activity":
                self._activity.delete_many({"Meeting_ID": {"$regex": region}})
                logger.debug("Current Activity CDO deleted for country {0}".format(region.pattern))
            if collection == "institution":
                self._institution.delete_many({"IM_CRM_Institution_ID": {"$regex": region}})
                logger.debug("Current Institution CDO deleted for country {0}".format(region.pattern))
            if collection == "contact":
                self._contact.delete_many({"C_IM_CRM_Security_Label1": {"$regex": region}})
                logger.debug("Current Contact deleted for region {0} ".format(region.pattern))
            # filter also by status
            if collection == "personActivity":
                self._personActivity.delete_many({"IM_CRM_Meeting_ID": {"$regex": region}})
                logger.debug("Current Person Activity deleted for country {0}".format(region.pattern))
            if collection == "personInstitution":
                self._personInstitution.delete_many({"IM_CRM_Institution_ID": {"$regex": region}})
                logger.debug("Current Person Institution deleted for country {0}".format(region.pattern))
            # past collections
            if collection == "activityPast":
                self._activityPast.delete_many({"Meeting_ID": {"$regex": region}})
                logger.debug("Past Activity CDO deleted for country {0}".format(region.pattern))
            if collection == "institutionPast":
                self._institutionPast.delete_many({"IM_CRM_Institution_ID": {"$regex": region}})
                logger.debug("Past Institution CDO deleted for country {0}".format(region.pattern))
            # filter also by status
            if collection == "contactPast":
                self._contactPast.delete_many({"C_IM_CRM_Security_Label1": {"$regex": region}})
                logger.debug("Past Contact deleted for region {0}".format(region.pattern))
            # all activities collection
            if collection == "allActivities":
                self._allActivities.delete_many({})
            # joined collections
            if collection == "joinedActivity":
                self._joinedActivity.delete_many({})
                logger.debug("Joined Activity CDO deleted")
            if collection == "joinedInstitution":
                self._joinedInstitution.delete_many({})
                logger.debug("Joined Institution CDO deleted")
        except:
            logger.debug("No data existed in {0} collection yet to be emptied for region {1}".format(collection, region))

    def move_data_past(self, collection, data, region):
        if collection == "activityPast":
            if data:
                # delete old past data
                self.delete_data(collection="activityPast", region=region)
                # archive current data
                self.insert_data(collection="activityPast", data=data)
            else:
                logger.debug("No new Activity CDO to archive")
            # empty current collections (raw and joined) awaiting for new data coming in
            self.delete_data(collection="activity", region=region)
            self.delete_data(collection="personActivity", region=region)
            self.delete_data(collection="joinedActivity")
            logger.debug("Activity CDO daily operations "
                         "(archive data and empty current collection) "
                         "complete for country {0}".format(region.pattern))
        if collection == "institutionPast":
            if data:
                self.delete_data(collection="institutionPast", region=region)
                self.insert_data(collection="institutionPast", data=data)
            else:
                logger.debug("No new Institution CDO to archive")
            self.delete_data(collection="institution", region=region)
            self.delete_data(collection="personInstitution", region=region)
            self.delete_data(collection="joinedInstitution")
            logger.debug("Institution CDO daily operations "
                         "(archive data and empty current collection) "
                         "complete for country {0}".format(region.pattern))
        if collection == "contactPast":
            if data:
                # delete old past data
                self.delete_data(collection="contactPast", region=region)
                # archive current data, marked status as imported to Eloqua
                self.insert_data(collection="contactPast", data=data)
            else:
                logger.debug("No new Contact to archive")
            # empty current collection awaiting for new data coming in
            self.delete_data(collection="contact", region=region)
            logger.debug("Contact daily operations "
                         "(archive data and empty current collection) "
                         "complete for region {0}".format(region.pattern))
