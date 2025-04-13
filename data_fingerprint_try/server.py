from io import StringIO
import warnings
import numbers

import streamlit as st
import polars as pl

from data_fingerprint.src.comparator import get_data_report
from data_fingerprint.src.models import DataReport, ColumnDifference
from data_fingerprint.src.utils import (
    get_dataframe,
    get_ratio_of_differences_per_source,
    get_column_difference_ratio,
    get_number_of_row_differences,
)

st.header("DataFingerprint")
st.markdown(
    """
    On this page you can try out the `data-fingerprint` Python library to see is it a good fit for
    your project.
    """
)

st.info(
    """
    The library is available on [PyPi](https://pypi.org/project/data-fingerprint/) and
    [GitHub](https://github.com/SimpleSimpler/data_fingerprint).
    """
)

st.info(
    """
    If you find any issues or bug, please reach out through GitHub
    """
)

st.markdown("-----")
st.markdown("## Try it out")


def get_differences(df1, df2, df1_name, df2_name, grouping_columns, thresholds):
    with warnings.catch_warnings(record=True) as w:
        with st.spinner("Generating report...", show_time=True):
            report: DataReport = get_data_report(
                df1, df2, df1_name, df2_name, grouping_columns, thresholds
            )

        st.markdown("### Raw Report")
        st.json(report.model_dump(), expanded=False)

        st.markdown("### Comparable columns")
        st.dataframe(report.comparable_columns)

        st.markdown("### Column differences")
        col_diffs: list[dict] = [cd.model_dump() for cd in report.column_differences]
        st.dataframe(col_diffs)

        st.markdown("### Row Differences")
        st.dataframe(get_dataframe(report))

        st.markdown("### Number of row difference")
        total_rows = (
            report.df0_length + report.df1_length
            if report.df0_length + report.df1_length > 0
            else 1
        )
        number_of_differences = get_number_of_row_differences(report)
        st.metric(
            label="Number of Row Differences",
            value=number_of_differences,
            delta_color="inverse" if number_of_differences > 0 else "normal",
            delta=f"{round((get_number_of_row_differences(report) / total_rows)*100, 4)} %",
        )
        st.info(
            "Delta is defined by dividing the number of row differences by the number of rows in the both data sources."
        )

        st.markdown("### Ratio of Row Difference Per Source")
        diff_row_ratio_per_source = get_ratio_of_differences_per_source(report)
        diff_row_info = {
            "source": diff_row_ratio_per_source.keys(),
            "ratio_of_differences": diff_row_ratio_per_source.values(),
            "color": ["#0000FF", "#FF0000"],
        }
        st.bar_chart(diff_row_info, x="source", y="ratio_of_differences", color="color")

        if grouping_columns is not None:
            st.markdown("### Ratio of Row Difference Per Column")

            diff_row_ratio_per_column = get_column_difference_ratio(report)

            diff_row_info = {
                "column": diff_row_ratio_per_column.keys(),
                "ratio_of_differences": diff_row_ratio_per_column.values(),
            }
            st.bar_chart(diff_row_info, x="column", y="ratio_of_differences")

            if w:
                for warning in w:
                    st.warning(warning.message)


upload_tab, example_tab = st.tabs(["Upload your data", "Use example data"])

with upload_tab:
    st.markdown("### Upload your data")

    col_0, col_1 = st.columns(2)

    with col_0:
        df0_name_upload: str = st.text_input(
            "Name of first data source", key="df0_name_upload"
        )
        df0_uploader = st.file_uploader("Upload first data source")

    with col_1:
        df1_name_upload: str = st.text_input(
            "Name of second data source", key="df1_name_upload"
        )
        df1_uploader = st.file_uploader("Upload second data source")

    grouping_columns_upload = st.text_input(
        "Grouping columns (comma separated)", key="grouping_columns_upload"
    )

    st.markdown("### Define thresholds")
    thresholds_upload = st.text_area(
        "Thresholds (combination of 'column_name=threshold', comma separated)",
        key="thresholds_upload",
    )

    calculate_differences_upload = st.button(
        "Calculate differences", key="calculate_differences_upload"
    )

    if calculate_differences_upload:
        grouping_columns_upload = [
            col.strip() for col in grouping_columns_upload.split(",")
        ]
        if grouping_columns_upload == [""]:
            grouping_columns_upload = None

        thresholds_upload = [
            threshold.strip() for threshold in thresholds_upload.split(",")
        ]
        thresholds_upload = {
            threshold.split("=")[0]: float(threshold.split("=")[1])
            for threshold in thresholds_upload
            if threshold != ""
        }

        if df0_uploader is None or df1_uploader is None:
            st.error("Please upload both data sources")
            st.stop()

        if df0_name_upload == "" or df1_name_upload == "":
            st.error("Please enter names for both data sources")
            st.stop()

        df1_data = pl.read_csv(df0_uploader)
        df2_data = pl.read_csv(df1_uploader)
        get_differences(
            df1_data,
            df2_data,
            df0_name_upload,
            df1_name_upload,
            grouping_columns_upload,
            thresholds_upload,
        )

with example_tab:
    st.markdown("### Use example data")

    col_0, col_1 = st.columns(2)

    with col_0:
        df0_name_raw: str = st.text_input(
            "Name of first data source", key="df0_name_raw"
        )
        df0_data_raw = st.text_area(
            label="First data", placeholder="Write down your CSV example"
        )

    with col_1:
        df1_name_raw: str = st.text_input(
            "Name of second data source", key="df1_name_raw"
        )
        df1_data_raw = st.text_area(
            label="Second data", placeholder="Write down your CSV example"
        )

    grouping_columns_raw = st.text_input(
        "Grouping columns (comma separated)", key="grouping_columns_raw"
    )

    st.markdown("### Define thresholds")
    thresholds_raw = st.text_area(
        "Thresholds (combination of 'column_name=threshold', comma separated)",
        key="thresholds_raw",
    )

    calculate_differences_raw = st.button(
        "Calculate differences", key="calculate_differences_raw"
    )

    if calculate_differences_raw:
        grouping_columns_raw = [col.strip() for col in grouping_columns_raw.split(",")]
        if grouping_columns_raw == [""]:
            grouping_columns_raw = None

        thresholds_raw = [threshold.strip() for threshold in thresholds_raw.split(",")]
        thresholds_raw = {
            threshold.split("=")[0]: float(threshold.split("=")[1])
            for threshold in thresholds_raw
            if threshold != ""
        }

        df1_data = pl.read_csv(StringIO(df0_data_raw))
        df2_data = pl.read_csv(StringIO(df1_data_raw))
        get_differences(
            df1_data,
            df2_data,
            df0_name_raw,
            df1_name_raw,
            grouping_columns_raw,
            thresholds_raw,
        )
