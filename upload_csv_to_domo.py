import csv
import logging

from pydomo import Domo, Column, ColumnType, Schema
from pydomo import DataSetRequest

from commons import Ticket_Data
from config.env import CLIENT_ID, CLIENT_SECRET,FreshDesk_Ticket_ID
from utils import send_email_error
from logger_config import setup_logging

# Setup logging
setup_logging(module_name="Upload Cost Data")
logger = logging.getLogger(__name__)

date_time_ticket = {'due_by', 'fr_due_by', 'created_at', 'updated_at','stats.agent_responded_at', 'stats.requester_responded_at', 'stats.first_responded_at', 'stats.status_updated_at', 'stats.reopened_at', 'stats.resolved_at', 'stats.closed_at','stats.pending_since' }



def get_new_column(file_path):
    with open(file_path, 'r') as file:
        csv_reader = csv.reader(file)
        headers = next(csv_reader)



def get_column_type(column_name, date_time_columns):
    if column_name in date_time_columns:
        return ColumnType.DATETIME
    else:
        return ColumnType.STRING




def upload_dataset(domo, dataset_id, file_path, date_time_columns, dataset_name, dataset_description):
    # Update a DataSet's metadata
    ds = domo.datasets
    update = DataSetRequest()

    with open(file_path, 'r') as file:
        csv_reader = csv.reader(file)
        headers = next(csv_reader)

    # update.schema = Schema([Column(get_column_type(col, date_time_columns), col) for col in headers])
    update.schema = Schema([Column(get_column_type(col, date_time_columns), col) for col in headers])
    update.name = dataset_name
    update.description = dataset_description

    ds.update(dataset_id, update)
    ds.data_import_from_file(dataset_id, file_path)
    logger.info("Uploaded data from a file to Dataset {}".format(dataset_id, dataset_name))


def upload_csv_to_domo_daily():
    try:
        datasets_info = [

            (FreshDesk_Ticket_ID,Ticket_Data,date_time_ticket,
             'Fresh Desk Ongoing ', 'Contains Last 30 days tickets info from Freshdesk'),

        ]
        domo = Domo(CLIENT_ID, CLIENT_SECRET, api_host='api.domo.com')

        for dataset_id, filepath, date_time_columns, dataset_name, dataset_description in datasets_info:
            upload_dataset(domo, dataset_id, filepath, date_time_columns, dataset_name,
                           dataset_description)
    except Exception as exc:
        send_email_error(exc)


if __name__ == "__main__":
    upload_csv_to_domo_daily()