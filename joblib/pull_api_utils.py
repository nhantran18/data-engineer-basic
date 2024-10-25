import os
import datetime


def add_timestamp_to_filename(original_filename):
    # Get the current date and time
    now = datetime.datetime.now()

    # Format the timestamp
    timestamp = now.strftime('%Y-%m-%d-%H_%M')

    # Split the original filename into name and extension
    name, extension = os.path.splitext(original_filename)

    # Create a new filename with the timestamp appended before the file extension
    new_filename = f"{name}_{timestamp}{extension}"

    return new_filename
