import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import re

from datetime import datetime, timedelta
from tqdm import tqdm


ORDERING_COLUMN = "source_timestamp"
SORT = True
ISUTC = True


def parse_kafka_text_file(path, verbose=False):
    """
    Convert a text file produced by PythonKafkaConsumers/mon_events_consumer.py into a pandas DataFrame.

    Parameters:
    - path (str): The path to the text file to be parsed.
    - verbose (bool, optional): Whether to print the entries while processing them or not. Default is False.

    Returns:
    - DataFrame: A pandas DataFrame containing the parsed records.

    Note:
    The text file should be formatted in a specific way for proper parsing.

    Example:
    If the file contains lines like 'message value: {"timestamp": 1644711514, "source_timestamp": 1644711512, "data": [1]}'.
    This function will parse these lines into a DataFrame.
    """

    START_TAG = "message value: "
    records = []

    with open(path) as file:
        for line in tqdm(file):
            if line.startswith("Successfully"):
                continue
            if line.startswith(START_TAG):
                # Safely evaluate the expression after the start tag
                record = eval(line[len(START_TAG) :])
                records.append(record)
            if verbose:
                print(line)

    # Iterate over records to process values
    for rec in tqdm(records):
        rec["dt"] = datetime.utcfromtimestamp(rec["timestamp"])
        rec["source_dt"] = datetime.utcfromtimestamp(rec["source_timestamp"])
        if rec["assembly"] in rec["name"]:
            rec["assembly_name"] = rec["name"]
            rec["name"] = rec["name"][len(rec["assembly"]) + 1 :]
        else:
            a = rec["assembly"]
            p = rec["name"]
            rec["assembly_name"] = f"{a}:{p}"

    return pd.DataFrame(records)


def read_cassandra_csv(path, time_decimals=3):
    """
    Read a CSV file exported from Cassandra and process it into a pandas DataFrame.

    Parameters:
    - path (str): The path to the CSV file.
    - time_decimals (int, optional): Number of decimals to round the timestamp values to. Default is 3.

    Returns:
    - DataFrame: A pandas DataFrame containing the processed data.

    Note:
    The CSV file should have 'data', 'timestamp', and 'source_timestamp' columns.
    """

    # Read CSV file into a DataFrame
    df = pd.read_csv(path)

    assembly_names = []
    names = []
    datas = []
    timestamps = []
    source_timestamps = []
    dts = []
    source_dts = []

    # Iterate over each row to process data and timestamps
    for i, (val, ts, src_ts, assembly, name) in enumerate(
        zip(
            df["data"].values,
            df["timestamp"].values,
            df["source_timestamp"].values,
            df["assembly"].values,
            df["name"].values,
        )
    ):
        datas.append(eval(val))

        # Process timestamps with optional rounding
        if time_decimals:
            timestamps.append(np.round(ts, decimals=time_decimals))
            source_timestamps.append(np.round(src_ts, decimals=time_decimals))
        else:
            timestamps.append(ts)
            source_timestamps.append(src_ts)

        dts.append(datetime.utcfromtimestamp(ts))
        source_dts.append(datetime.utcfromtimestamp(src_ts))

        if assembly in name:
            assembly_names.append(name)
            names.append(name[len(assembly) + 1 :])
        else:
            assembly_names.append(f"{assembly}:{name}")
            names.append(name)

    # Update DataFrame with processed values
    df["data"] = datas
    df["timestamp"] = timestamps
    df["source_timestamp"] = source_timestamps
    df["timestamp"] = df["timestamp"].astype(float)
    df["source_timestamp"] = df["source_timestamp"].astype(float)
    df["dt"] = dts
    df["source_dt"] = source_dts
    df["assembly_name"] = assembly_names
    df["name"] = names

    return df


def transform_simulator_timestamp_to_isoformat(values):
    """
    Transform a list of simulator timestamp strings (UTC) into ISO 8601 format.

    Parameters:
    - values (list): A list of timestamp strings in the format "MM/DD/YY HH:MM:SS".

    Returns:
    - list: A list of timestamp strings converted to ISO 8601 format ("YYYY-MM-DDTHH:MM:SS").

    Example:
    If values = ["01/23/24 12:34:56", "02/13/24 01:23:45"], this function will return
    ["2024-01-23T12:34:56", "2024-02-13T01:23:45"].
    """
    formatted_values = []

    for v in values:
        time = v[9:21]
        m = v[:2]
        d = v[3:5]
        y = "20" + v[6:8]
        formatted_values.append(y + "-" + m + "-" + d + "T" + time)

    return formatted_values


def convert_simulators_timestamps(isoformat_values, isUTC=ISUTC, is_solar_time=True):
    """
    Convert a list of isoformat timestamps to unix and to datetime object.

    Parameters:
    - isoformat_values (list): A list of timestamp strings in ISO 8601 format.
    - isUTC (bool, optional): Whether the provided isoformat_value is UTC or not. Default is ISUTC.
    - is_solar_time (bool, optional): Whether the solar time is active or not. Default is True.

    Returns:
    - list: A list of unix timestamps;
    - list: A list of isoformat timestamps.
    """
    unix, isoformat = [], []

    for v in isoformat_values:

        t = convert_from_isoformat_to_unix(v, isUTC=isUTC, is_solar_time=is_solar_time)

        unix.append(t)
        isoformat.append(datetime.utcfromtimestamp(t))

    return unix, isoformat


def convert_from_isoformat_to_unix(isoformat_value, isUTC=ISUTC, is_solar_time=True):
    """
    Convert an isoformat timestamp to a unix datetime object.

    Parameters:
    - isoformat_value (str): A timestamp string in ISO 8601 format.
    - isUTC (bool, optional): Whether the provided isoformat_value is UTC or not. Default is ISUTC.
    - is_solar_time (bool, optional): Whether the solar time is active or not. Default is True.

    Returns:
    - float: The converted timestamp as a Unix timestamp (seconds since the epoch).
    """
    if isUTC:
        # Adjust hours to UTC
        time_offset = 1 if is_solar_time else 2
        isoformat_value = (
            isoformat_value[:11]
            + str(int(isoformat_value[11:13]) + time_offset).zfill(2)
            + isoformat_value[13:]
        )

    # Convert ISO 8601 formatted value to a Unix timestamp
    return datetime.fromisoformat(isoformat_value).timestamp()


def reformat_csv_files(path_folder, write_file=False):
    """
    Reformat CSV files in a folder by removing unnecessary elements and adding identifiers.

    Parameters:
    - path_folder (str): The path to the folder containing CSV files.
    - write_file (bool, optional): Whether to write reformatted files to disk. Default is False.

    Returns:
    - list: A list of reformatted rows from all CSV files in the folder.
    """
    all_rows = []

    # Iterate over files in the folder
    for path in tqdm(os.listdir(path_folder)):
        # Check if the file is not already reformatted
        if "reformatted" not in path:
            # Extract file index
            i = re.search(r"\d+(?=\.[^.]+$)", path).group()

            # Read and process the input file
            with open(os.path.join(path_folder, path), "r") as input_file:
                input_values = input_file.read().strip().split(",")
                values = [
                    val.replace("\n", "")
                    for val in input_values
                    if val not in ("---------", "")
                ]
                assert len(values) % 4 == 0
                rows = [values[j : j + 4] for j in range(0, len(values), 4)]

                # Write to output file if specified
                if write_file:
                    output_file = open(
                        os.path.join(path_folder, f"reformatted_{path}"), "w"
                    )

                # Process rows, add identifier, and append to all_rows list
                for row in rows:
                    row[2] = row[2].replace(
                        "2:", ""
                    )  # Remove '2:' from the third column
                    row.append(f"WS{i}")  # Add identifier
                    all_rows.append(row)
                    if write_file:
                        output_file.write(",".join(row) + "\n")

                # Close the output file if specified
                if write_file:
                    output_file.close()

    return all_rows


def get_simulators_dataframes(
    path_folder, from_file=False, isUTC=ISUTC, is_solar_time=True
):
    """
    Generate DataFrames containing simulator data from CSV files.

    Parameters:
    - path_folder (str): The path to the folder containing CSV files.
    - from_file (bool, optional): Whether to read from reformatted CSV files. Default is False.
    - isUTC (bool, optional): Whether the simulators times are UTC or not. Default is ISUTC.
    - is_solar_time (bool, optional): Whether the solar time is active or not. Default is True.

    Returns:
    - DataFrame: A pandas DataFrame containing the combined and processed simulator data.
    """
    if from_file:
        dataframes = []
        for path in tqdm(os.listdir(path_folder)):
            if "reformatted" in path:
                df = pd.read_csv(os.path.join(path_folder, path), header=None)
                dataframes.append(df)
        combined_df = pd.concat(dataframes, ignore_index=True)
    else:
        dataframes = reformat_csv_files(path_folder)
        combined_df = pd.DataFrame(dataframes)

    combined_df.columns = [
        "ServerTimestamp",
        "SourceTimestamp",
        "name",
        "data",
        "assembly",
    ]

    combined_df["data"] = [
        [eval(val.capitalize())] for val in combined_df["data"].values
    ]

    # Transform timestamp formats
    server_timestamps = transform_simulator_timestamp_to_isoformat(
        combined_df["ServerTimestamp"].values
    )
    source_timestamps = transform_simulator_timestamp_to_isoformat(
        combined_df["SourceTimestamp"].values
    )

    # Convert timestamps
    combined_df["timestamp"], combined_df["dt"] = convert_simulators_timestamps(
        server_timestamps, isUTC=isUTC, is_solar_time=is_solar_time
    )
    (
        combined_df["source_timestamp"],
        combined_df["source_dt"],
    ) = convert_simulators_timestamps(
        source_timestamps, isUTC=isUTC, is_solar_time=is_solar_time
    )

    assembly_names = []
    for i in range(len(combined_df)):
        a = combined_df["assembly"].iloc[i]
        p = combined_df["name"].iloc[i]
        assembly_names.append(f"{a}:{p}")

    combined_df["assembly_name"] = assembly_names

    return combined_df


def add_assembly_name_column(dataframe):
    """
    Add a new column named 'assembly_name' to the provided DataFrame.

    The 'assembly_name' column is created by concatenating the values in the 'assembly' and 'name' columns
    of the DataFrame, separated by a colon.

    Parameters:
    - dataframe (DataFrame): The DataFrame to which the 'assembly_name' column will be added.

    Returns:
    - DataFrame: The DataFrame with the 'assembly_name' column added.
    """
    dataframe["assembly_name"] = (
        dataframe[["assembly", "name"]].agg(":".join, axis=1).values
    )
    return dataframe


def get_timestamps_and_data(dataframe, value):
    """
    Return timestamp, and source_timestamp fields for a for a given monitored property and assembly in the provided DataFrame
    as numpy ndarrays.

    Parameters:
    - dataframe (DataFrame): The DataFrame containing the monitored data.
    - value (str): The name of the value to retrieve.

    Returns:
    - tuple: A tuple containing three numpy ndarrays: (timestamp, source_timestamp).
    """
    try:
        idx = np.where(dataframe["assembly_name"] == value)
    except KeyError:
        print("WARNING: column `assembly_name` not in dataframe. Creating it...")
        dataframe = add_assembly_name_column(dataframe)

    return (
        np.array(dataframe["timestamp"].iloc[idx]),
        np.array(dataframe["source_timestamp"].iloc[idx]),
    )


def get_restricted_dataframe(df, start_time, end_time, column=ORDERING_COLUMN):
    """
    Return a subset of the DataFrame with rows filtered based on the specified time range.

    Parameters:
    - df (DataFrame): The DataFrame to be filtered.
    - start_time (str or Timestamp): The start time of the time range (inclusive).
    - end_time (str or Timestamp): The end time of the time range (exclusive).
    - column (str, optional): The column name representing the timestamps. Default is ORDERING_COLUMN.

    Returns:
    - DataFrame: A subset of the original DataFrame containing rows within the specified time range.
    """
    return df[(df[column] > start_time) & (df[column] < end_time)]


def get_sorted_subset_dataframe(
    df, columns, by=ORDERING_COLUMN, ascending=True, drop=True
):
    """
    Return a sorted subset of the DataFrame with selected columns, based on a specified sorting column.

    Parameters:
    - df (DataFrame): The DataFrame to be filtered and sorted.
    - columns (list): The list of column names to include in the subset.
    - by (str, optional): The column name for sorting. Default is ORDERING_COLUMN.
    - ascending (bool, optional): Whether to sort in ascending order. Default is True.
    - drop (bool, optional): Whether to drop the index after sorting. Default is True.


    Returns:
    - DataFrame: A sorted subset of the original DataFrame containing selected columns.
    """
    return df[columns].sort_values(by=by, ascending=ascending).reset_index(drop=drop)


def get_mean_std(df, assembly_property_name, column=ORDERING_COLUMN, sort=SORT):
    """
    Calculate the mean and standard deviation of time differences between consecutive timestamps
    for each monitored property in the provided DataFrame.

    Parameters:
    - df (DataFrame): The DataFrame containing the monitored data.
    - assembly_property_name (str): The name of the assembly and the property to consider in the form assembly:property.
    - column (str, optional): The column name representing the timestamps. Only "timestamp" and "source_timestamp" are admitted. Default is ORDERING_COLUMN.
    - sort (bool, optional): Whether to sort timestamps before calculating differences. Default is SORT.

    Returns:
    - tuple: A tuple containing two lists: (mean, std), where mean and std are the mean and standard deviation
             of time differences for each monitored property.
    """
    ts, src_ts = get_timestamps_and_data(df, assembly_property_name)

    if sort:
        ts.sort()
        src_ts.sort()

    if column == "source_timestamp":
        diff = np.diff(src_ts)
    elif column == "timestamp":
        diff = np.diff(ts)
    else:
        raise ValueError(f"Invalid column name: {column}")

    return np.mean(diff), np.std(diff)


def get_mean_std_for_assembly_and_property(df, column=ORDERING_COLUMN, sort=SORT):
    """
    Calculate the mean and standard deviation of time differences between consecutive timestamps
    for each monitored property gruoped by the assemblies in the provided DataFrame.

    Parameters:
    - df (DataFrame): The DataFrame containing the monitored data.
    - column (str, optional): The column name representing the timestamps. Default is ORDERING_COLUMN.
    - sort (bool, optional): Whether to sort timestamps before calculating differences. Default is SORT.

    Returns:
    - tuple: A tuple containing two ndarrays: (mean, std), where mean and std are the mean and standard deviation
             of time differences for each monitored property.
    """
    # df = add_assembly_name_column(df)

    unique_assemblies_and_properties = df["assembly_name"].unique()

    mean = np.zeros(len(unique_assemblies_and_properties))
    std = np.zeros(len(unique_assemblies_and_properties))

    for i, a_p in enumerate(unique_assemblies_and_properties):
        m, s = get_mean_std(df, a_p, column=column, sort=sort)
        mean[i] = m
        std[i] = s

    return mean, std


def get_aggregate_difference_distribution(
    dataframes, column=ORDERING_COLUMN, sort=SORT
):
    """
    Calculate the mean and standard deviation of aggregate differences for each property of a given assembly
    in the provided dataframes.

    Parameters:
    - dataframes (list of DataFrame): A list of DataFrame objects containing the data.

    Returns:
    - means_stds (list of float): A list containing the mean and standard deviation of aggregate differences
      for each property of a given assembly in the provided dataframes. The list has the structure: [mean1, std1, mean2, std2, ...],
      where each element is a numpy array.
    """
    means_stds = []

    for df in dataframes:
        mean, std = get_mean_std_for_assembly_and_property(df, column=column, sort=sort)
        means_stds.extend([mean, std])

    return means_stds


def plot_histogram(data, title=None, number_of_bins=None):
    """
    Plot the histogram of the given values.

    Parameters:
    - data (array-like): The array-like object containing the values to plot the histogram for.
    - title (str, optional): The title of the plot. Default is None.
    - number_of_bins (int, optional): The number of bins for the histogram. If not provided, bins are automatically computed.

    Returns:
    - None
    """
    plt.figure()
    plt.title(title)
    plt.hist(data, bins=number_of_bins)
    plt.show(block=False)


def get_time_differences(dataframes, assembly_property_name, sort=SORT):
    """
    Calculate time differences between consecutive timestamps in multiple dataframes.

    Parameters:
    - dataframes (list of DataFrame): A list of DataFrame objects containing the data.
    - assembly_property_name (str): The name of the assembly and the property to consider in the form assembly:property.
    - sort (bool, optional): Whether to sort the timestamps before computing differences. Default is True.

    Returns:
    - time_diffs (list of array-like): A list containing the time differences between consecutive timestamps
      and source timestamps for each dataframe provided. The list has the structure:
      [diff_ts_df1, diff_src_ts_df1, diff_ts_df2, diff_src_ts_df2, ...], where each element is a numpy array
    """
    time_diffs = []

    for df in dataframes:
        ts, src_ts = get_timestamps_and_data(df, assembly_property_name)
        if sort:
            ts.sort()
            src_ts.sort()

        time_diffs.extend([np.diff(ts), np.diff(src_ts)])

    return time_diffs


def get_sorted_colum_values(dataframes, column=ORDERING_COLUMN):
    """
    Extract the values of a specified column from multiple dataframes and return them as a sorted list.

    Parameters:
    - dataframes (list of DataFrame): A list of DataFrame objects containing the data.
    - column (str, optional): The name of the column whose values are to be extracted. Default is ORDERING_COLUMN.

    Returns:
    - data_sorted_selected (list of array-like): A list containing the values of the specified column
      from each dataframe provided. Each element in the list is a numpy array representing the values
      extracted from a single dataframe.
    """
    data_sorted_selected = []

    for df in dataframes:
        d = get_sorted_subset_dataframe(df, [column], by=column)
        data_sorted_selected.append(d[column].values)

    return data_sorted_selected


def get_number_of_bins(data, number_of_bins):
    """
    Generate a list specifying the number of bins for histograms.

    Parameters:
    - data (list or array-like): A list or array-like object containing the data.
    - number_of_bins (int or list of int or None): The number of bins for histograms.
      If int, the same number of bins is used for all histograms. If list, it should have the same length as data.
      If None, None is returned for each element in the returned list.

    Returns:
    - bins (list of int or None): A list specifying the number of bins for histograms.
      If number_of_bins is an integer, the list contains that integer repeated len(data) times.
      If number_of_bins is None, the list contains None repeated len(data) times.
    """
    if number_of_bins is None:
        return [None] * len(data)
    elif isinstance(number_of_bins, int):
        return [number_of_bins] * len(data)


def plot_one_axes_subplot(
    subplot_titles, title, data, labels, n_bins, rotation, size, labels_pos
):
    """
    Plot histograms in a single row of subplots.

    Parameters:
    - subplot_titles (list): A list of titles for each subplot.
    - title (str): The title of the entire figure.
    - data (list): A list of data arrays to plot as histograms. Each array corresponds to a subplot.
    - labels (list of str): The labels to display along respectively the x and the y axis.
    - n_bins (list): A list of integers indicating the number of bins for each subplot.
    - rotation (int): Rotation angle for y-axis labels.
    - size (str): Font size for y-axis labels.
    - labels_pos (list of lists): The [x, y] positions of the x and y lables, respectively.
    """
    fig, axs = plt.subplots(1, len(subplot_titles))
    fig.suptitle(title)

    for i, (d, t, b) in enumerate(zip(data, subplot_titles, n_bins)):
        axs[i].hist(d, bins=b)
        axs[i].set_title(t)

    fig.text(labels_pos[0][0], labels_pos[0][1], labels[0], ha="center", size=size)
    fig.text(
        labels_pos[1][0],
        labels_pos[1][1],
        labels[1],
        va="center",
        rotation=rotation,
        size=size,
    )

    fig.tight_layout()
    plt.show()


def plot_two_axes_subplot(
    subplot_titles,
    title,
    data,
    n_bins,
    labels,
    y_tags,
    rotation,
    size,
    y_tags_pos,
    labels_pos,
):
    """
    Plot histograms with two y-axes for each subplot title.

    Parameters:
    - subplot_titles (list of str): A list of subplot titles corresponding to each column in the data.
    - title (str): The title of the overall plot.
    - data (list of array-like): A list containing data for histograms. Each element in the list represents
      the data for a subplot.
    - n_bins (list of int): A list specifying the number of bins for histograms for each subplot.
    - labels (list of str): The labels to display along respectively the x and the y axis.
    - y_tags (list of str): Tags for y-axes of the subplots.
    - rotation (int): Rotation angle for y-axis labels.
    - size (str): Font size for y-axis labels.
    - y_tags_pos (list): Positions [x, y] for the tags for y-axes of the subplots.
    - labels_pos (list): The [x, y] positions of the x and y lables, respectively.
    """
    fig, axs = plt.subplots(2, len(subplot_titles))
    fig.suptitle(title)

    for i in range(len(subplot_titles)):
        k = 2 * i
        axs[0, i].hist(data[k], bins=n_bins[k])
        axs[1, i].hist(data[k + 1], bins=n_bins[k + 1])

    # Add labels for subplot columns and rows
    for ax, col in zip(axs[0], subplot_titles):
        ax.set_title(col)

    for ax, row in zip(axs[:, 0], y_tags):
        ax.set_ylabel(row, rotation=rotation, size=size)
        ax.yaxis.set_label_coords(y_tags_pos[0], y_tags_pos[1])

    fig.text(labels_pos[0][0], labels_pos[0][1], labels[0], ha="center", size=size)
    fig.text(
        labels_pos[1][0],
        labels_pos[1][1],
        labels[1],
        va="center",
        rotation=rotation,
        size=size,
    )

    fig.tight_layout()
    plt.show()


def plot_time_differences(
    dataframes_dict,
    assembly_name,
    property_name,
    number_of_bins=None,
    sort=SORT,
    plot_lables=["consecutive_samples_difference", "number_of_data"],
    y_tags=["timestamp", "source_timestamp"],
    rotation=90,
    size="large",
    y_tags_pos=[-0.3, 0.5],
    labels_pos=[[0.5, -0.01], [-0.01, 0.5]],
):
    """
    Plot histograms of time timestamps and source timestamps differences for a given assembly and property name
    in the provided set of dataframes.

    Parameters:
    - dataframes_dict (dict): A dictionary where keys represent names or identifiers for DataFrames,
      and values are the DataFrame objects themselves.
    - assembly_name (str): The name of the assembly for which to filter data.
    - property_name (str): The name of the monitored property for which time differences will be plotted.
    - number_of_bins (int or list of int, optional): The number of bins for the histograms.
      If int, the same number of bins is used for all histograms. If list, it should have the same length as data.
      Default is None, which means bins are automatically computed for each histogram.
    - sort (bool, optional): Whether to sort timestamps before calculating differences. Default is SORT.
    - plot_labels (list of str): The labels to display along respectively the x and the y axis.
    - y_tags (list of str, optional): Tags for y-axis of the subplots. Default is ['timestamp', 'source_timestamp'].
    - rotation (int, optional): Rotation angle for y-axis labels. Default is 90.
    - size (str, optional): Font size for y-axis labels. Default is "large".
    - y_tags_pos (list): Positions [x, y] for the tags for y-axes of the subplots. Default is [-0.3, 0.5].
    - labels_pos (list of lists): The [x, y] positions of the x and y lables, respectively. Default is [[0.5, -0.01], [-0.01, 0.5]].
    """
    subplot_titles = dataframes_dict.keys()
    dataframes = dataframes_dict.values()

    assembly_property_name = ":".join([assembly_name, property_name])
    data = get_time_differences(dataframes, assembly_property_name, sort=sort)

    title = f"Time difference distributions for {assembly_name}:{property_name}"
    n_bins = get_number_of_bins(data, number_of_bins)

    plot_two_axes_subplot(
        subplot_titles,
        title,
        data,
        n_bins,
        plot_lables,
        y_tags,
        rotation,
        size,
        y_tags_pos,
        labels_pos,
    )


def plot_aggregate_distributions(
    dataframes_dict,
    column=ORDERING_COLUMN,
    number_of_bins=None,
    sort=SORT,
    plot_lables=["aggregate_time_differences", "number_of_properties"],
    y_tags=["mean_values", "standard_deviations"],
    rotation=90,
    size="large",
    y_tags_pos=[-0.3, 0.5],
    labels_pos=[[0.5, -0.01], [-0.01, 0.5]],
):
    """
    Plot histograms of time timestamps and source timestamps differences for a given assembly and property name
    in the provided set of dataframes.

    Parameters:
    - dataframes_dict (dict): A dictionary where keys represent names or identifiers for DataFrames,
      and values are the DataFrame objects themselves.
    - column (str, optional): The label for the y-axis. Default is ORDERING_COLUMN.
    - number_of_bins (int or list of int, optional): The number of bins for the histograms.
      If int, the same number of bins is used for all histograms. If list, it should have the same length as data.
      Default is None, which means bins are automatically computed for each histogram.
    - sort (bool, optional): Whether to sort timestamps before calculating differences. Default is SORT.
    - plot_labels (list of str): The labels to display along respectively the x and the y axis.
    - y_tags (list of str, optional): Tags for y-axis of the subplots. Default is ["mean_values", "standard_deviation"].
    - rotation (int, optional): Rotation angle for y-axis labels. Default is 90.
    - size (str, optional): Font size for y-axis labels. Default is "large".
    - y_tags_pos (list): Positions [x, y] for the tags for y-axes of the subplots. Default is [-0.3, 0.5].
    - labels_pos (list of lists): The [x, y] positions of the x and y lables, respectively. Default is [[0.5, -0.01], [-0.01, 0.5]].
    """
    subplot_titles = dataframes_dict.keys()
    dataframes = dataframes_dict.values()

    data = get_aggregate_difference_distribution(dataframes, column=column, sort=sort)

    stamp_number_of_unique_assemblies_properties_in_dataframe(subplot_titles, data)

    print(
        f"The following plot represents the distribution of the {y_tags[0]} and {y_tags[1]} of the \n"
        f"{column} differences for each unique couple of assembly and property."
    )
    print()
    print(
        "N.B. If all the properties were sampled at 1 Hz, \n"
        f"the expected distribution of the {y_tags[0]} for \n"
        "all the monitored properties should be a Gaussian \n"
        f"centered at 1 sec with a {y_tags[1][:-1]} of milliseconds.\n"
    )

    title = f"Aggregate {column} difference distributions for each property of a given assembly"
    n_bins = get_number_of_bins(data, number_of_bins)
    plot_two_axes_subplot(
        subplot_titles,
        title,
        data,
        n_bins,
        plot_lables,
        y_tags,
        rotation,
        size,
        y_tags_pos,
        labels_pos,
    )


def get_time_range_for_each_property_name(dataframes, columns, column):
    """
    Computes the acquisition time range for each unique couple assembly and property across multiple dataframes.

    Parameters:
    - dataframes (list): A list of pandas DataFrames containing the data.
    - columns (list): A list of column names that should be considered while computing the time range.
    - column (str): The column representing the timestamp or time-related data.

    Returns:
    - properties_acquisition_time_ranges_in_dataframes (list): A list containing the time ranges for
      each unique couple assembly and property across all provided dataframes.
    """
    properties_acquisition_time_ranges_in_dataframes = []

    for df in dataframes:

        d = get_sorted_subset_dataframe(df, columns, by=column)
        assembly_properties = d["assembly_name"].unique()

        properties_acquisition_time_ranges = []
        for a_p in assembly_properties:
            idx = np.where(d["assembly_name"] == a_p)
            times = np.array(df[column].iloc[idx])
            properties_acquisition_time_ranges.append(max(times) - min(times))

        properties_acquisition_time_ranges_in_dataframes.append(
            np.array(properties_acquisition_time_ranges)
        )

    return properties_acquisition_time_ranges_in_dataframes


def plot_data_rate_histogram(
    dataframes_dict,
    column=ORDERING_COLUMN,
    number_of_bins=100,
    rotation=90,
    size="large",
    labels_pos=[[0.5, -0.01], [-0.01, 0.5]],
):
    """
    Plot histograms of the specified `column` for each dataframe.
    Each bin corresponds to the number of properties collected in the bin timerange. Default is ORDERING_COLUMN.

    Parameters:
    - dataframes_dict (dict): A dictionary where keys represent names or identifiers for DataFrames,
      and values are the DataFrame objects themselves.
    - title (str, optional): The title of the overall plot. Default is None.
    - column (str, optional): The label for the y-axis. Default is ORDERING_COLUMN.
    - number_of_bins (int, optional): The number of bins for the histograms. Default is 100.
    - rotation (int, optional): Rotation angle for y-axis labels. Default is 90.
    - size (str, optional): Font size for y-axis labels. Default is "large".
    - labels_pos (list of lists): The [x, y] positions of the x and y lables, respectively. Default is [[0.5, -0.01], [-0.01, 0.5]].
    """
    subplot_titles = dataframes_dict.keys()
    dataframes = dataframes_dict.values()

    data_sorted_selected = get_sorted_colum_values(dataframes, column=column)
    assert len(data_sorted_selected) == len(subplot_titles)

    print(
        f"The following plot represents the histogram of the {column}. \n"
        "Each bin corresponds to the number of data collected in the bin timerange."
    )

    title = f"{column} data rate distribution"
    n_bins = get_number_of_bins(data_sorted_selected, number_of_bins)

    plot_one_axes_subplot(
        subplot_titles,
        title,
        data_sorted_selected,
        [column, "number_of_entries"],
        n_bins,
        rotation=rotation,
        size=size,
        labels_pos=labels_pos,
    )


def plot_acquisition_duration_per_property(
    dataframes_dict,
    acquisition_duration_sec,
    column=ORDERING_COLUMN,
    number_of_bins=None,
    rotation=90,
    size="large",
    labels_pos=[[0.5, -0.01], [-0.01, 0.5]],
):
    """
    Plot histograms of the specified `column` for each dataframe.

    Parameters:
    - dataframes_dict (dict): A dictionary where keys represent names or identifiers for DataFrames,
      and values are the DataFrame objects themselves.
    - acquisition_duration_sec (int): Time global duration of the acquisition in seconds.
    - column (str, optional): The label for the y-axis. Default is ORDERING_COLUMN.
    - number_of_bins (int, optional): The number of bins for the histograms. Default is None.
    - rotation (int, optional): Rotation angle for y-axis labels. Default is 90.
    - size (str, optional): Font size for y-axis labels. Default is "large".
    - labels_pos (list of lists): The [x, y] positions of the x and y lables, respectively. Default is [[0.5, -0.01], [-0.01, 0.5]].
    """
    subplot_titles = dataframes_dict.keys()
    dataframes = dataframes_dict.values()

    columns = [column, "assembly_name"]

    properties_acquisition_time_ranges_in_dataframes = (
        get_time_range_for_each_property_name(dataframes, columns, column)
    )
    assert len(properties_acquisition_time_ranges_in_dataframes) == len(dataframes)

    print(
        "The following plot represents the distribution of the acquisition duration for \n"
        "each unique couple assembly and property."
    )
    print()
    print(
        f"N.B. in the ideal case there should be only one bin centered at {acquisition_duration_sec}"
    )

    title = "Acquisition duration per unique assembly and property distribution"
    n_bins = get_number_of_bins(
        properties_acquisition_time_ranges_in_dataframes, number_of_bins
    )
    plot_one_axes_subplot(
        subplot_titles,
        title,
        properties_acquisition_time_ranges_in_dataframes,
        ["acquisition_duration", "number_of_properties"],
        n_bins,
        rotation=rotation,
        size=size,
        labels_pos=labels_pos,
    )


def stamp_first_and_last_timestamp(dataframes_dict, column="source_dt"):
    """
    Print the first and last timestamps for each DataFrame in the given dictionary.

    Parameters:
    - dataframes_dict (dict): A dictionary where keys represent names or identifiers for DataFrames,
      and values are the DataFrame objects themselves.
    - column (str, optional): The name of the column containing timestamps against with you want to perform the query. Default is "source_dt".
    """
    for k in dataframes_dict:
        print(f"{k} dataframe timerange")
        print(min(dataframes_dict[k][column]))
        print(max(dataframes_dict[k][column]))
        print()


def stamp_expected_number_of_rows(
    number_of_monitored_properties,
    acquisition_duration_sec,
    sampling_frequencies_hz=1,
    fill_chars=13,
):
    """
    Print the theoretical number of entries in each dataframe.

    The expected number of entries in each dataframe is given by the number
    of monitored properties sampled at 1 Hz for the time duration of the simulation.

    Parameters:
    - number_of_monitored_properties (list or int): Number of monitored properties.
    - acquisition_duration_sec (int): Time duration of the acquisition in seconds.
    - sampling_frequencies_hz (int or list): List of sampling frequencies associated to each
      monitored property in Hertz. If `sampling_frequencies_hz` is an integer, it creates a
      list of that integer repeated as many times as the number of monitored properties. Default is 1 Hz.
    - fill_chars (int, optional): The width for padding the DataFrame names in the printed output.
      Default is 13.
    """
    if isinstance(sampling_frequencies_hz, int):
        sampling_frequencies_hz = [
            sampling_frequencies_hz
        ] * number_of_monitored_properties

    assert number_of_monitored_properties == len(sampling_frequencies_hz)

    print("The theoretical number of entries in dataframes for selected timerange is: ")
    print(
        "".zfill(fill_chars).replace("0", " ")
        + f"{round(sum(sampling_frequencies_hz) * acquisition_duration_sec)}"
    )
    print()


def stamp_number_of_rows_in_dataframe(dataframes_dict, fill_chars=12):
    """
    Print the number of rows in each DataFrame in the given dictionary.

    Parameters:
    - dataframes_dict (dict): A dictionary where keys represent names or identifiers for DataFrames,
      and values are the DataFrame objects themselves.
    - fill_chars (int, optional): The width for padding the DataFrame names in the printed output.
      Default is 12.
    """
    print("Number of rows in the selected timerange for dataframes:")
    for k in dataframes_dict:
        print(f"{k}: ".zfill(fill_chars).replace("0", " "), len(dataframes_dict[k]))
    print()


def stamp_number_of_unique_assemblies_properties_in_dataframe(
    subplot_titles, data, fill_chars=12
):
    """
    Print the number of unique assemblies and properties in each DataFrame in the given dictionary.

    Parameters:
    - dataframes_dict (dict): A dictionary where keys represent names or identifiers for DataFrames,
      and values are the DataFrame objects themselves.
    - data (list): A list containing the mean and standard deviation of aggregate differences
      for each property of a given assembly in the provided dataframes. The list has the structure: [mean1, std1, mean2, std2, ...],
      where each element is a numpy array.
    - fill_chars (int, optional): The width for padding the DataFrame names in the printed output.
      Default is 12.
    """
    assert len(data) == 2 * len(subplot_titles)

    print("Number of unique couples of assembly and property in dataframes:")
    for i, t in enumerate(subplot_titles):
        print(f"{t}: ".zfill(fill_chars).replace("0", " "), len(data[2 * i]))
    print()


def get_interval_duration(start_time, end_time, verbose=True):
    """
    Calculate the duration between two timestamps in seconds.

    Parameters:
    - start_time (str): Start timestamp in ISO format (YYYY-MM-DDTHH:MM:SS) or (YYYY-MM-DD HH:MM:SS).
    - end_time (str): End timestamp in ISO format (YYYY-MM-DDTHH:MM:SS) or (YYYY-MM-DD HH:MM:SS).
    - verbose (bool, optional): Whether to print the results on stdout or not. Default is True.

    Returns:
    -  interval_duration (float) : Duration between `start_time` and `end_time` in seconds.
    """
    a = datetime.fromisoformat(start_time)
    b = datetime.fromisoformat(end_time)
    interval_duration = (b - a).total_seconds()
    if verbose:
        print(
            f"The acquisition interval duration in second is: {interval_duration} "
            f"(about {round(interval_duration / 60)} min)"
        )
        print()
    return interval_duration
