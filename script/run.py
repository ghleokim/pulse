import process_file
import json

REPO_ROOT="./data"
OUTPUT_ROOT="./output"

FIRST_DIRS=["aggregated","map","top"]
DATA_INNER_PATH="country/india"
SECOND_DIRS=["insurance", "transaction", "user"]

SRC_DIRS = [ f"{REPO_ROOT}/{first_dir}/{second_dir}/{DATA_INNER_PATH}" for first_dir in FIRST_DIRS for second_dir in SECOND_DIRS ]
OUTPUT_DIRS = [ f"{OUTPUT_ROOT}/{first_dir}/{second_dir}" for first_dir in FIRST_DIRS for second_dir in SECOND_DIRS ]

SKIP_IF_STATE = lambda x: "state" in x
SKIP_IF_NOT_STATE = lambda x: "state" not in x

"""aggregated/insurance"""
def aggregated_insurance_or_transaction(src_file_path, json_data):
    csv_data = []

    # get date and state name from directory path
    splitted_file_path = src_file_path.split("/")
    date = f'{splitted_file_path[-2]}-{splitted_file_path[-1].split(".")[0]}Q'
    
    # Extract data from hoverDataList
    for name_data in json_data['data']['transactionData']:
        name = name_data['name']
        # Loop through each metric (though we expect only one based on given data)
        for metric in name_data['paymentInstruments']:
            if metric['type'] == 'TOTAL':  # Only consider 'TOTAL' type
                number_of_transactions = metric['count']
                value_of_transactions = metric['amount']
                # Add data to CSV rows
                csv_data.append([date, name, number_of_transactions, value_of_transactions])
    
    return csv_data

"""aggregated/insurance/.../state"""
def aggregated_insurance_or_transaction_state(src_file_path, json_data):
    csv_data = []

    # get date and state name from directory path
    splitted_file_path = src_file_path.split("/")
    state = splitted_file_path[-3]
    date = f'{splitted_file_path[-2]}-{splitted_file_path[-1].split(".")[0]}Q'
    
    # Extract data from hoverDataList
    for name_data in json_data['data']['transactionData']:
        name = name_data['name']
        # Loop through each metric (though we expect only one based on given data)
        for metric in name_data['paymentInstruments']:
            if metric['type'] == 'TOTAL':  # Only consider 'TOTAL' type
                number_of_transactions = metric['count']
                value_of_transactions = metric['amount']
                # Add data to CSV rows
                csv_data.append([state, date, name, number_of_transactions, value_of_transactions])
    
    return csv_data


def aggregated_user(src_file_path, json_data):
    csv_data = []

    # get date and state name from directory path
    splitted_file_path = src_file_path.split("/")
    date = f'{splitted_file_path[-2]}-{splitted_file_path[-1].split(".")[0]}Q'
    
    # Extract data from hoverDataList
    try:
        for metric in json_data['data']['usersByDevice']:
            brand = metric['brand']
            count = metric['count']
            percentage = metric['percentage']
            # Add data to CSV rows
            csv_data.append([date, brand, count, percentage])
    except TypeError:
        csv_data.append([date, "null", "null", "null"])
    
    return csv_data

"""aggregated/user/.../state"""
def aggregated_user_state(src_file_path, json_data):
    csv_data = []

    # get date and state name from directory path
    splitted_file_path = src_file_path.split("/")
    state = splitted_file_path[-3]
    date = f'{splitted_file_path[-2]}-{splitted_file_path[-1].split(".")[0]}Q'
    
    # Extract data from hoverDataList
    try:
        for metric in json_data['data']['usersByDevice']:
            brand = metric['brand']
            count = metric['count']
            percentage = metric['percentage']
            # Add data to CSV rows
            csv_data.append([state, date, brand, count, percentage])
    except TypeError:
        csv_data.append([state, date, "null", "null", "null"])
    
    return csv_data

def aggregated_user_aggregated(src_file_path, json_data):
    csv_data = []
    # get date and state name from directory path
    splitted_file_path = src_file_path.split("/")
    date = f'{splitted_file_path[-2]}-{splitted_file_path[-1].split(".")[0]}Q'
    
    # Extract data from hoverDataList
    try:
        metric = json_data['data']['aggregated']
        registeredUsers = metric['registeredUsers']
        appOpens = metric['appOpens']
        # Add data to CSV rows
        csv_data.append([date, registeredUsers, appOpens])
    except TypeError:
        csv_data.append([date, "null", "null", "null"])
    return csv_data

def aggregated_user_state_aggregated(src_file_path, json_data):
    csv_data = []
    # get date and state name from directory path
    splitted_file_path = src_file_path.split("/")
    state = splitted_file_path[-3]
    date = f'{splitted_file_path[-2]}-{splitted_file_path[-1].split(".")[0]}Q'
    
    # Extract data from hoverDataList
    try:
        metric = json_data['data']['aggregated']
        registeredUsers = metric['registeredUsers']
        appOpens = metric['appOpens']
        # Add data to CSV rows
        csv_data.append([state, date, registeredUsers, appOpens])
    except TypeError:
        csv_data.append([state, date, "null", "null", "null"])
    return csv_data

### MAP ###
def map_insurance_or_transaction(src_file_path, json_data):
    csv_data = []

    # get date and state name from directory path
    splitted_file_path = src_file_path.split("/")
    date = f'{splitted_file_path[-2]}-{splitted_file_path[-1].split(".")[0]}Q'
    
    # Extract data from hoverDataList
    for hoverDataList in json_data['data']['hoverDataList']:
        name = hoverDataList['name']
        # Loop through each metric (though we expect only one based on given data)
        for metric in hoverDataList['metric']:
            if metric['type'] == 'TOTAL':  # Only consider 'TOTAL' type
                count = metric['count']
                amount = metric['amount']
                # Add data to CSV rows
                csv_data.append([date, name, count, amount])
    
    return csv_data

def map_insurance_or_transaction_state(src_file_path, json_data):
    csv_data = []

    # get date and state name from directory path
    splitted_file_path = src_file_path.split("/")
    state = splitted_file_path[-3]
    date = f'{splitted_file_path[-2]}-{splitted_file_path[-1].split(".")[0]}Q'
    
    # Extract data from hoverDataList
    for hoverDataList in json_data['data']['hoverDataList']:
        name = hoverDataList['name']
        # Loop through each metric (though we expect only one based on given data)
        for metric in hoverDataList['metric']:
            if metric['type'] == 'TOTAL':  # Only consider 'TOTAL' type
                count = metric['count']
                amount = metric['amount']
                # Add data to CSV rows
                csv_data.append([state, date, name, count, amount])
    
    return csv_data

def map_user(src_file_path, json_data):
    csv_data = []

    # get date and state name from directory path
    splitted_file_path = src_file_path.split("/")
    date = f'{splitted_file_path[-2]}-{splitted_file_path[-1].split(".")[0]}Q'
    
    # Extract data from hoverDataList
    for key, item in json_data['data']['hoverData'].items():
        name = key
        registeredUsers = item['registeredUsers']
        appOpens = item['appOpens']
        csv_data.append([date, name, registeredUsers, appOpens])
    
    return csv_data

def map_user_state(src_file_path, json_data):
    csv_data = []

    # get date and state name from directory path
    splitted_file_path = src_file_path.split("/")
    state = splitted_file_path[-3]
    date = f'{splitted_file_path[-2]}-{splitted_file_path[-1].split(".")[0]}Q'
    
    # Extract data from hoverDataList
    for key, item in json_data['data']['hoverData'].items():
        name = key
        registeredUsers = item['registeredUsers']
        appOpens = item['appOpens']
        csv_data.append([state, date, name, registeredUsers, appOpens])
    
    return csv_data

def top_insurance(src_file_path, json_data):
    csv_data = []

    print(src_file_path)

    # get date and state name from directory path
    splitted_file_path = src_file_path.split("/")
    date = f'{splitted_file_path[-2]}-{splitted_file_path[-1].split(".")[0]}Q'
    
    # Extract data from hoverDataList
    for category in ["states", "districts", "pincodes"]:
        for item in json_data['data'][category]:
            entityName = item['entityName']
            count = item['metric']['count']
            amount = item['metric']['amount']
            csv_data.append([date, category, entityName, count, amount])

    return csv_data

def top_insurance_state(src_file_path, json_data):
    csv_data = []

    # get date and state name from directory path
    splitted_file_path = src_file_path.split("/")
    state = splitted_file_path[-3]
    date = f'{splitted_file_path[-2]}-{splitted_file_path[-1].split(".")[0]}Q'
    
    # Extract data from hoverDataList
    for category in ["districts", "pincodes"]:
        for item in json_data['data'][category]:
            entityName = item['entityName']
            count = item['metric']['count']
            amount = item['metric']['amount']
            csv_data.append([state, date, category, entityName, count, amount])

    return csv_data

def top_user(src_file_path, json_data):
    csv_data = []

    print(src_file_path)

    # get date and state name from directory path
    splitted_file_path = src_file_path.split("/")
    date = f'{splitted_file_path[-2]}-{splitted_file_path[-1].split(".")[0]}Q'
    
    # Extract data from hoverDataList
    for category in ["states", "districts", "pincodes"]:
        for item in json_data['data'][category]:
            entityName = item['name']
            registeredUsers = item['registeredUsers']
            csv_data.append([date, category, entityName, registeredUsers])

    return csv_data

def top_user_state(src_file_path, json_data):
    csv_data = []

    # get date and state name from directory path
    splitted_file_path = src_file_path.split("/")
    state = splitted_file_path[-3]
    date = f'{splitted_file_path[-2]}-{splitted_file_path[-1].split(".")[0]}Q'
    
    # Extract data from hoverDataList
    for category in ["districts", "pincodes"]:
        for item in json_data['data'][category]:
            entityName = item['name']
            registeredUsers = item['registeredUsers']
            csv_data.append([state, date, category, entityName, registeredUsers])

    return csv_data



function_map = {
    "aggregated": {
        "insurance": {
            "name": "aggregated_insurance",
            "function": lambda x: process_file.read_json_and_process_data(x, aggregated_insurance_or_transaction, SKIP_IF_STATE),
            # "function": aggregated_insurance,
            "header": "date, name, count, amount\n",
            "state": {
                "name": "state_aggregated_insurance",
                "function": lambda x: process_file.read_json_and_process_data(x, aggregated_insurance_or_transaction_state),
                "header": "state, date, name, count, amount\n"
            }
        },
        "transaction": {
            "name": "aggregated_transaction",
            "function": lambda x: process_file.read_json_and_process_data(x, aggregated_insurance_or_transaction, SKIP_IF_STATE),
            "header": "date, name, count, amount\n",
            "state": {
                "name": "state_aggregated_transaction",
                "function": lambda x: process_file.read_json_and_process_data(x, aggregated_insurance_or_transaction_state),
                "header": "state, date, name, count, amount\n"
            }
        },
        "user": {
            "name": "aggregated_user_byIndividualDevices",
            "function": lambda x: process_file.read_json_and_process_data(x, aggregated_user),
            "header": "date, brand, count, percentage\n",
            # extra: additional functions to run
            "extra": [
                {
                    "name": "aggregated_user_aggregated",
                    "function": lambda x: process_file.read_json_and_process_data(x, aggregated_user_aggregated, SKIP_IF_STATE),
                    "header": "date, registeredUsers, appOpens\n"
                }
            ],
            "state": {
                "name": "state_aggregated_user_byIndividualDevices",
                "function": lambda x: process_file.read_json_and_process_data(x, aggregated_user_state),
                "header": "state, date, brand, count, percentage\n",
                "extra": [
                    {
                        "name": "state_aggregated_user_aggregated",
                        "function": lambda x: process_file.read_json_and_process_data(x, aggregated_user_state_aggregated),
                        "header": "state, date, registeredUsers, appOpens\n"
                    }
                ]
            }
        }
    },
    "map": {
        "insurance": {
            "name": "map_insurance",
            "function": lambda x: process_file.read_json_and_process_data(x, map_insurance_or_transaction, SKIP_IF_STATE),
            "header": "date, name, count, amount\n",
            "state": {
                "name": "state_map_insurance",
                "function": lambda x: process_file.read_json_and_process_data(x, map_insurance_or_transaction_state),
                "header": "state, date, name, count, amount\n",
            },
        },
        "transaction": {
            "name": "map_transaction",
            "function": lambda x: process_file.read_json_and_process_data(x, map_insurance_or_transaction, SKIP_IF_STATE),
            "header": "date, name, count, amount\n",
            "state": {
                "name": "state_map_transaction",
                "function": lambda x: process_file.read_json_and_process_data(x, map_insurance_or_transaction_state),
                "header": "state, date, name, count, amount\n",
            },
        },
        "user": {
            "name": "map_user",
            "function": lambda x: process_file.read_json_and_process_data(x, map_user, SKIP_IF_STATE),
            "header": "date, name, registeredUsers, appOpens",
            "state": {
                "name": "state_map_user",
                "function": lambda x: process_file.read_json_and_process_data(x, map_user_state),
                "header": "state, date, name, registeredUsers, appOpens",
            },
        },
    },
    "top": {
        "insurance": {
            "name": "top_insurance",
            "function": lambda x: process_file.read_json_and_process_data(x, top_insurance, SKIP_IF_STATE),
            "header": "date, category, entityName, count, amount\n",
            "state": {
                "name": "state_top_insurance",
                "function": lambda x: process_file.read_json_and_process_data(x, top_insurance_state),
                "header": "state, date, category, entityName, count, amount\n"
            }
        },
         "transaction": {
            "name": "top_transaction",
            "function": lambda x: process_file.read_json_and_process_data(x, top_insurance, SKIP_IF_STATE),
            "header": "date, category, entityName, count, amount\n",
            "state": {
                "name": "state_top_transaction",
                "function": lambda x: process_file.read_json_and_process_data(x, top_insurance_state),
                "header": "state, date, category, entityName, count, amount\n"
            }
        },
         "user": {
            "name": "top_user",
            "function": lambda x: process_file.read_json_and_process_data(x, top_user, SKIP_IF_STATE),
            "header": "date, category, name, registeredUsers\n",
            "state": {
                "name": "state_top_user",
                "function": lambda x: process_file.read_json_and_process_data(x, top_user_state),
                "header": "state, date, category, name, registeredUsers\n"
            }
        }
    }
}

def main():
    # process_file.copy_and_process_files(f"{REPO_ROOT}/aggregated/{INSURANCE_PATH}", f"{OUTPUT_ROOT}/aggregated/{INSURANCE_PATH}", function_map["aggregated"]["insurance"]["function"], function_map["aggregated"]["insurance"]["header"])

    count=1
    for first_dir in ["aggregated", "map", "top"]:
        for second_dir in ["insurance", "transaction", "user"]:
            if not function_map.get(first_dir) or not function_map[first_dir].get(second_dir): 
                continue

            print(f"\n{count}. {first_dir} - {second_dir}")

            print("  👉 run for plain data")
            if first_dir == "map":
                src_dir = f"{REPO_ROOT}/{first_dir}/{second_dir}/hover/{DATA_INNER_PATH}"
            else:
                src_dir = f"{REPO_ROOT}/{first_dir}/{second_dir}/{DATA_INNER_PATH}"
                
            dest_dir = f"{OUTPUT_ROOT}/{first_dir}/{second_dir}"

            item = function_map[first_dir][second_dir]
            dest_filename = item["name"]
            func = item["function"]
            header = item["header"]

            process_file.copy_and_process_files(src_dir, dest_dir, dest_filename, func, header)

            extras = function_map[first_dir][second_dir].get("extra", [])
            for extra in extras:
                dest_filename=extra["name"]
                header = extra["header"]
                process_file.copy_and_process_files(src_dir, dest_dir, dest_filename, func, header)


            print("  👉 run for state data")
            if first_dir == "map":
                src_dir = f"{REPO_ROOT}/{first_dir}/{second_dir}/hover/{DATA_INNER_PATH}/state"
            else:
                src_dir = f"{REPO_ROOT}/{first_dir}/{second_dir}/{DATA_INNER_PATH}/state"
            dest_dir = f"{OUTPUT_ROOT}/{first_dir}/{second_dir}"

            state_item = function_map[first_dir][second_dir]["state"]
            dest_filename = state_item["name"]
            func = state_item["function"]
            header = state_item["header"]

            process_file.copy_and_process_files(src_dir, dest_dir, dest_filename, func, header)

            extras = function_map[first_dir][second_dir]["state"].get("extra", [])
            for extra in extras:
                dest_filename=extra["name"]
                func = extra["function"]
                header = extra["header"]
                process_file.copy_and_process_files(src_dir, dest_dir, dest_filename, func, header)

            count+=1
    
if __name__ == "__main__":
    main()