#! /bin/bash
#
# USAGE:
#

PATH=${PATH}:/bin;

app=$(basename $0);

# Usage message
USAGE="
NAME
    $app - 

SYNOPSIS
    $app [h]

DESCRIPTION
    -h
        show help message
";

# Default values for options

# Process options
while getopts "h" option
do
    case "$option" in
        "h")
            echo -e "$USAGE";
            exit 0;
            ;;
        "?")
            echo -e "$USAGE" >&2;
            exit 1;
            ;;
    esac
done

# Remove option from $@
shift $((OPTIND-1));

if [ "$#" -eq 0 ]
then
    echo "No request file specified" >&2;
    exit 1;
fi

. ${HOME}/.bashrc;

. $VIRTUALENVWRAPPER_SCRIPT;

workon core

export PATH=${HOME}/git/uframe-async-requests:${PATH};

for f in "$@"
do
	# File containing the requests and associated metadata.  Create this file
	# using send_async_requests_from_urlcsv.py > queued_requests.csv
	REQUEST_CSV=$f;
	# Create a temporary file to write the results of the checks to
	REQUEST_QUEUE_TMP_CSV=$(mktemp);

#    echo "REQUEST CSV: $REQUEST_CSV";
#    echo "TEMP REQUEST CSV: $REQUEST_QUEUE_TMP_CSV";
#    continue

	if [ ! -f "$REQUEST_CSV" ]
	then
	    echo "Request queue CSV not found: $REQUEST_CSV";
	    return 1;
	fi
	
	# Check the request status
	check_async_requests_from_csv.py $REQUEST_CSV > $REQUEST_QUEUE_TMP_CSV;
	
	# Move $REQUEST_QUEUE_TMP_CSV to $REQUEST_CSV
	mv $REQUEST_QUEUE_TMP_CSV $REQUEST_CSV;

done
	
deactivate;
	
