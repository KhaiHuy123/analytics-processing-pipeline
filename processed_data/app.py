
from functions import *

setup_config(
    page_title="Taxi NYC",
    page_icon=":bar_chart:",
    title=":blue[**TAXI_NYC_ANALYTICS**]",
    markdown="_TLC (Taxi Limousine Commission)_"
)

feature_1, feature_2 = st.columns(2, gap="large", vertical_alignment="top")
create_logo("logo_taxi.jpg")

# ----------------------------------------------------------------------------------------------------------------------

with feature_1:
    create_header("_PickUp/DropOff Location_")
    option = create_select_box(
        label="Choose Service",
        list_elem=["GREEN", "YELLOW", "FHV"],
        key="service_type"
    )
    setup_option = create_select_box(
        label="Choose Metrics",
        list_elem=["Zone", "Borough"],
        key="setup_option"
    )
    if setup_option == "Zone":
        list_elem = ["Zone", "Borough"]
    else:
        list_elem = ["Borough", "Zone"]

    # ------------------------------------------------------------------------------------------------------------------

    if option == "YELLOW":
        create_sub_header(header=":orange[**YELLOW**]", divider='orange')
        option_y_pickUp_dropOff = create_select_box(
            label="Choose Information",
            list_elem=["Pick Up", "Drop Off"],
            key="option_y_pickUp_dropOff",
        )

        # --------------------------------------------------------------------------------------------------------------

        if option_y_pickUp_dropOff == "Pick Up":
            popular_y_pickUp_df = upload_data(
                key="yellow_popular_pick_up_location",
                direction_str="yellow_popular_pick_up_location",
                extension="parquet",
                file_path="./yellowpopularpickuplocation/yellowpopularpickuplocation.parquet",
                show_data=False
            )
            y_pick_up_list_borough = define_gy_location_filter(
                label="Filter Yellow Picking Up Locations",
                data=popular_y_pickUp_df,
                col_filter="borough"
            )
            filtered_data_y_pickUp = filter_data(
                data=popular_y_pickUp_df,
                referenced_elem=popular_y_pickUp_df.columns[0:1],
                selected_elem=[
                    y_pick_up_list_borough
                ],
                show_data=False
            )

            # ----------------------------------------------------------------------------------------------------------

            if setup_option == "Zone":
                st.write("Yellow Pick UP Location Statistics By Zone")
                plot_bar_chart(
                    data=filtered_data_y_pickUp,
                    x="zone",
                    y="num_of_picking_up_locations",
                    x_label="zone",
                    y_label="num_of_picking_up",
                    horizontal=False,
                )
                create_zone_markdown()
                create_zone_details_markdown()
            else:
                st.write("Yellow Pick Up Location Statistics By Borough")
                plot_bar_chart(
                    data=filtered_data_y_pickUp,
                    x="borough",
                    y="num_of_picking_up_locations",
                    x_label="borough",
                    y_label="num_of_picking_up",
                )
                create_borough_markdown()

        # --------------------------------------------------------------------------------------------------------------

        else:
            popular_y_dropOff_df = upload_data(
                key='yellow_popular_drop_off_location',
                direction_str="yellow_popular_drop_off_location",
                extension='parquet',
                file_path="./yellowpopulardropofflocation/yellowpopulardropofflocation.parquet",
                show_data=False
            )
            y_drop_off_list_borough = define_gy_location_filter(
                label="Filter Yellow Dropping Off Locations",
                data=popular_y_dropOff_df,
                col_filter="borough"
            )
            filtered_data_y_dropOff = filter_data(
                data=popular_y_dropOff_df,
                referenced_elem=popular_y_dropOff_df.columns[0:1],
                selected_elem=[
                    y_drop_off_list_borough
                ],
                show_data=False
            )

            # ----------------------------------------------------------------------------------------------------------

            if setup_option == "Zone":
                st.write("Yellow Drop Off Location Statistics By Zone")
                plot_bar_chart(
                    data=filtered_data_y_dropOff,
                    x="zone",
                    y="num_of_dropping_off_locations",
                    x_label="zone",
                    y_label="num_of_dropping_off",
                    horizontal=False,
                )
                create_zone_markdown()
                create_zone_details_markdown()
            else:
                st.write("Yellow Drop Off Location Statistics By Borough")
                plot_bar_chart(
                    data=filtered_data_y_dropOff,
                    x="borough",
                    y="num_of_dropping_off_locations",
                    x_label="borough",
                    y_label="num_of_dropping_off",
                )
                create_borough_markdown()

        # --------------------------------------------------------------------------------------------------------------

        distance_moving_y_df = upload_data(
            key='yellow_distance_moving',
            direction_str="yellow_distance_moving",
            extension='parquet',
            caption="Distance Moving",
            file_path="./yellowdistancemoving/yellowdistancemoving.parquet",
            show_data=True
        )

        # --------------------------------------------------------------------------------------------------------------

        y_inner_outer_zone_option = create_select_box(
            label="Show In/Out Zone Trips",
            list_elem=["In Zone", "Out Zone"],
            key="g_distance_moving_option"
        )
        if y_inner_outer_zone_option == "In Zone":
            yt_in_zone_moving_df = distance_moving_y_df[distance_moving_y_df
                                                        ["pulocationid"]
                                                        == distance_moving_y_df["dolocationid"]]
            st.write("**Trend Of Moving For In-Zone Trips**")
            plot_line_chart(
                data=yt_in_zone_moving_df,
                x="pulocationid",
                y="distance_moving_average",
                x_label="locartion id",
                y_label="distance_moving_average"
            )
        else:
            yt_out_zone_moving_df = distance_moving_y_df[distance_moving_y_df
                                                         ["pulocationid"]
                                                         != distance_moving_y_df["dolocationid"]]
            st.write("**Trend Of Moving For Out-Zone Trips**")
            plot_bar_chart(
                data=yt_out_zone_moving_df,
                x="pulocationid",
                y="distance_moving_average",
                x_label="locartion id",
                y_label="distance_moving_average",
                horizontal=False
            )

        # --------------------------------------------------------------------------------------------------------------

        y_long_short_distance_option = create_select_box(
            label="Distance Moving For Long/Shot Distance",
            list_elem=["Long Distance", "Short Distance"],
            key="g_long_short_distance_option"
        )
        if y_long_short_distance_option == "Long Distance":
            yt_long_distance_moving_df = distance_moving_y_df[distance_moving_y_df
                                                              ["distance_moving_average"] >= 100]
            plot_line_single_variable_chart(
                data=yt_long_distance_moving_df,
                column="distance_moving_average",
                title="Trend Of Long Distance Moving Average",
            )
            create_explanation_distance_moving_details_markdown()
        else:
            yt_short_distance_moving_df = distance_moving_y_df[distance_moving_y_df
                                                               ["distance_moving_average"] < 100]
            plot_line_single_variable_chart(
                data=yt_short_distance_moving_df,
                column="distance_moving_average",
                title="Trend Of Short Distance Moving Average",
            )
            create_explanation_distance_moving_details_markdown()

        # --------------------------------------------------------------------------------------------------------------

        y_activity_option = create_select_box(
            label="Picking Up Or Dropping Off Action At 1 Zone ",
            list_elem=["Picking Up", "Dropping Off"],
            key="g_activity_option"
        )
        if y_activity_option == "Picking Up":
            plot_histogram(
                data=distance_moving_y_df,
                column="pulocationid",
                bins=150,
                x_label="locartion id",
                y_label="Num Of Picking Up",
                title="Picking Up Actions Per Taxi-Zone"
            )
        else:
            plot_histogram(
                data=distance_moving_y_df,
                column="dolocationid",
                bins=150,
                x_label="locartion id",
                y_label="Num Of Dropping Off",
                title="Dropping Off Actions Per Taxi-Zone"
            )

    # ------------------------------------------------------------------------------------------------------------------

    elif option == "GREEN":
        create_sub_header(header=":green[**GREEN**]", divider='green')

        option_g_pickUp_dropOff = create_select_box(
            label="Choose Information",
            list_elem=["Pick Up", "Drop Off"],
            key="option_g_pickUp_dropOff",
        )

        # --------------------------------------------------------------------------------------------------------------

        if option_g_pickUp_dropOff == "Pick Up":
            popular_g_pickUp_df = upload_data(
                key='green_popular_pick_up_location',
                direction_str="green_popular_pick_up_location",
                extension='parquet',
                file_path="./greenpopularpickuplocation/greenpopularpickuplocation.parquet",
                show_data=False
            )
            g_pick_up_list_borough = define_gy_location_filter(
                label="Filter Green Picking Up Locations",
                data=popular_g_pickUp_df,
                col_filter="borough"
            )
            filtered_data_g_pickUp = filter_data(
                data=popular_g_pickUp_df,
                referenced_elem=popular_g_pickUp_df.columns[0:1],
                selected_elem=[
                    g_pick_up_list_borough
                ],
                show_data=False
            )

            # ----------------------------------------------------------------------------------------------------------

            if setup_option == "Zone":
                st.write("**Green Pick Up Location Statistics By Zone**")
                plot_bar_chart(
                    data=filtered_data_g_pickUp,
                    x="zone",
                    y="num_of_picking_up_locations",
                    x_label="zone",
                    y_label="num_of_picking_up",
                    horizontal=False
                )
                create_zone_markdown()
                create_zone_details_markdown()
            else:
                st.write("**Green Pick Up Location Statistics By Borough**")
                plot_bar_chart(
                    data=filtered_data_g_pickUp,
                    x="borough",
                    y="num_of_picking_up_locations",
                    x_label="borough",
                    y_label="num_of_picking_up"
                )
                create_borough_markdown()

        # --------------------------------------------------------------------------------------------------------------

        else:
            popular_g_dropOff_df = upload_data(
                key='green_popular_drop_off_location',
                direction_str="green_popular_drop_off_location",
                extension='parquet',
                file_path="./greenpopulardropofflocation/greenpopulardropofflocation.parquet",
                show_data=False
            )
            g_drop_off_list_borough = define_gy_location_filter(
                label="Filter Green Dropping Off  Locations",
                data=popular_g_dropOff_df,
                col_filter="borough"
            )
            filtered_data_g_dropOff = filter_data(
                data=popular_g_dropOff_df,
                referenced_elem=popular_g_dropOff_df.columns[0:1],
                selected_elem=[
                    g_drop_off_list_borough
                ],
                show_data=False
            )

            # ----------------------------------------------------------------------------------------------------------

            if setup_option == "Zone":
                st.write("**Green Drop Off Location Statistics By Zone**")
                plot_bar_chart(
                    data=filtered_data_g_dropOff,
                    x="zone",
                    y="num_of_dropping_off_locations",
                    x_label="zone",
                    y_label="num_of_dropping_off",
                    horizontal=False
                )
                create_zone_markdown()
                create_zone_details_markdown()
            else:
                st.write("**Green Drop Off Location Statistics By Borough**")
                plot_bar_chart(
                    data=filtered_data_g_dropOff,
                    x="borough",
                    y="num_of_dropping_off_locations",
                    x_label="borough",
                    y_label="num_of_dropping_off"
                )
                create_borough_markdown()

        # --------------------------------------------------------------------------------------------------------------

        distance_moving_g_df = upload_data(
            key='green_distance_moving',
            direction_str="green_distance_moving",
            extension='parquet',
            caption="Distance Moving",
            file_path="./greendistancemoving/greendistancemoving.parquet",
            show_data=True
        )

        # --------------------------------------------------------------------------------------------------------------

        g_inner_outer_zone_option = create_select_box(
            label="Show In/Out Zone Trips",
            list_elem=["In Zone", "Out Zone"],
            key="g_distance_moving_option"
        )
        if g_inner_outer_zone_option == "In Zone":
            gt_in_zone_moving_df = distance_moving_g_df[distance_moving_g_df
                                                        ["pulocationid"]
                                                        == distance_moving_g_df["dolocationid"]]
            st.write("**Trend Of Moving For In-Zone Trips**")
            plot_line_chart(
                data=gt_in_zone_moving_df,
                x="pulocationid",
                y="distance_moving_average",
                x_label="locartion id",
                y_label="distance_moving_average"
            )
        else:
            gt_out_zone_moving_df = distance_moving_g_df[distance_moving_g_df
                                                         ["pulocationid"]
                                                         != distance_moving_g_df["dolocationid"]]
            st.write("**Trend Of Moving For Out-Zone Trips**")
            plot_bar_chart(
                data=gt_out_zone_moving_df,
                x="pulocationid",
                y="distance_moving_average",
                x_label="locartion id",
                y_label="distance_moving_average",
                horizontal=False
            )

        # --------------------------------------------------------------------------------------------------------------

        g_long_short_distance_option = create_select_box(
            label="Distance Moving For Long/Shot Distance",
            list_elem=["Long Distance", "Short Distance"],
            key="g_long_short_distance_option"
        )
        if g_long_short_distance_option == "Long Distance":
            gt_long_distance_moving_df = distance_moving_g_df[distance_moving_g_df
                                                              ["distance_moving_average"] >= 100]
            plot_line_single_variable_chart(
                data=gt_long_distance_moving_df,
                column="distance_moving_average",
                title="Trend Of Long Distance Moving Average",
            )
            create_explanation_distance_moving_details_markdown()
        else:
            gt_short_distance_moving_df = distance_moving_g_df[distance_moving_g_df
                                                               ["distance_moving_average"] < 100]
            plot_line_single_variable_chart(
                data=gt_short_distance_moving_df,
                column="distance_moving_average",
                title="Trend Of Short Distance Moving Average",
            )
            create_explanation_distance_moving_details_markdown()

        # --------------------------------------------------------------------------------------------------------------

        g_activity_option = create_select_box(
            label="Picking Up Or Dropping Off Action At 1 Zone ",
            list_elem=["Picking Up", "Dropping Off"],
            key="g_activity_option"
        )
        if g_activity_option == "Picking Up":
            plot_histogram(
                data=distance_moving_g_df,
                column="pulocationid",
                bins=150,
                x_label="locartion id",
                y_label="Num Of Picking Up",
                title="Picking Up Actions Per Taxi-Zone"
            )
        else:
            plot_histogram(
                data=distance_moving_g_df,
                column="dolocationid",
                bins=150,
                x_label="locartion id",
                y_label="Num Of Dropping Off",
                title="Dropping Off Actions Per Taxi-Zone"
            )

    # ------------------------------------------------------------------------------------------------------------------

    elif option == "FHV":
        create_sub_header(header=":red[**FHV (For Hire Vehicles)**]", divider='red')
        option_fhv_pickUp_dropOff = create_select_box(
            label="Choose Information",
            list_elem=["Pick Up", "Drop Off"],
            key="option_fhv_pickUp_dropOff"
        )

        # --------------------------------------------------------------------------------------------------------------

        if option_fhv_pickUp_dropOff == "Pick Up":
            popular_fhv_pickUp_df = upload_data(
                key='fhv_popular_pick_up_location',
                direction_str="fhv_popular_pick_up_location",
                extension='parquet',
                file_path="./fhvpopularpickuplocation/fhvpopularpickuplocation.parquet",
                show_data=False
            )
            fhv_pick_up_location = define_fhv_location_filter(
                label="Filter FHV Picking Up Locations",
                data=popular_fhv_pickUp_df,
                col_filter="borough",
            )
            filtered_data_fhv_pickUp = filter_data(
                data=popular_fhv_pickUp_df,
                referenced_elem=popular_fhv_pickUp_df.columns[0:1],
                selected_elem=[
                    fhv_pick_up_location
                ],
                show_data=False
            )

            # ----------------------------------------------------------------------------------------------------------

            if setup_option == "Zone":
                st.write("**FHV Pick Up Location Statistics By Zone**")
                plot_bar_chart(
                    data=filtered_data_fhv_pickUp,
                    x="zone",
                    y="num_of_picking_up",
                    x_label="zone",
                    y_label="num_of_picking_up",
                    horizontal=False
                )
                create_zone_markdown()
                create_zone_details_markdown()
            else:
                st.write("**FHV Pick Up Location Statistics By Borough**")
                plot_bar_chart(
                    data=filtered_data_fhv_pickUp,
                    x="borough",
                    y="num_of_picking_up",
                    x_label="borough",
                    y_label="num_of_picking_up"
                )
                create_borough_markdown()

        # --------------------------------------------------------------------------------------------------------------

        else:
            popular_fhv_dropOff_df = upload_data(
                key='fhv_popular_drop_off_location',
                direction_str="fhv_popular_drop_off_location",
                extension='parquet',
                file_path="./fhvpopulardropofflocation/fhvpopulardropofflocation.parquet",
                show_data=False
            )
            fhv_drop_off_location = define_fhv_location_filter(
                label="Filter FHV Dropping Off Locations",
                data=popular_fhv_dropOff_df,
                col_filter="borough",
            )
            filtered_data_fhv_dropOff = filter_data(
                data=popular_fhv_dropOff_df,
                referenced_elem=popular_fhv_dropOff_df.columns[0:1],
                selected_elem=[
                    fhv_drop_off_location
                ],
                show_data=False
            )

            # ----------------------------------------------------------------------------------------------------------

            if setup_option == "Zone":
                st.write("**FHV Drop Off Location Statistics By Zone**")
                plot_bar_chart(
                    data=filtered_data_fhv_dropOff,
                    x="zone",
                    y="num_of_dropping_off",
                    x_label="zone",
                    y_label="num_of_dropping_off",
                    horizontal=False
                )
                create_zone_markdown()
                create_zone_details_markdown()
            else:
                st.write("**FHV Drop Off Location Statistics By Borough**")
                plot_bar_chart(
                    data=filtered_data_fhv_dropOff,
                    x="borough",
                    y="num_of_dropping_off",
                    x_label="borough",
                    y_label="num_of_dropping_off"
                )
                create_borough_markdown()

        # --------------------------------------------------------------------------------------------------------------

        time_moving_fhv_df = upload_data(
            key='fhv_time_moving',
            direction_str="fhv_time_moving",
            extension='parquet',
            file_path='./fhvtriptimemoving/fhvtriptimemoving.parquet',
            show_data=False
        )
        fhv_time_moving_option = create_select_box(
            label="Time Moving For Trips In/Out Zone",
            list_elem=["In Zone", "Out Zone"],
            key="fhv_time_moving_option"
        )
        if fhv_time_moving_option == "In Zone":
            in_zone_moving_df = time_moving_fhv_df[
                time_moving_fhv_df
                ['pulocationid'] == time_moving_fhv_df['dolocationid']]
            plot_histogram(
                data=in_zone_moving_df,
                column="avg_time_serving",
                bins=150,
                x_label="locartion id",
                y_label="AVG_Time_Serving",
                title="Time Serving For FHV In-Zone Trips"
            )
        else:
            out_zone_moving_df = time_moving_fhv_df[
                time_moving_fhv_df
                ['pulocationid'] != time_moving_fhv_df['dolocationid']]
            plot_histogram(
                data=out_zone_moving_df,
                column="avg_time_serving",
                bins=150,
                x_label="locartion id",
                y_label="AVG_Time_Serving",
                title="Time Serving For FHV Out-Zone Trips"
            )

        # --------------------------------------------------------------------------------------------------------------

        trips_per_month_fhv_df = upload_data(
            key='trips_per_month',
            direction_str="trips_per_month",
            extension='parquet',
            file_path='./fhvtotaltripspermonth/fhvtotaltripspermonth.parquet',
            show_data=False
        )
        trips_per_year_fhv_df = upload_data(
            key='trips_per_year',
            direction_str="trips_per_year",
            extension='parquet',
            file_path='./fhvtotaltripsperyear/fhvtotaltripsperyear.parquet',
            show_data=False
        )

        # --------------------------------------------------------------------------------------------------------------

        fhv_trend_option = create_select_box(
            label="Trend Of FHV Trips",
            list_elem=["Uniques Vehicles", "Dispatched Trips", "Shared Trips"],
            key="fhv_trend_option"
        )
        if fhv_trend_option == "Uniques Vehicles":
            plot_line_single_variable_chart(
                data=trips_per_month_fhv_df,
                column="unique_vehicles",
                title="Trend of FHV Unique Vehicles - (2020 - Now)",
            )
        elif fhv_trend_option == "Dispatched Trips":
            plot_line_single_variable_chart(
                data=trips_per_month_fhv_df,
                column="total_dispatched_trips",
                title="Trend of FHV Dispatched Trips- (2020 - Now)",
            )
            st.write("**FHV Dispatched_Trips Per Year**")
            plot_bar_chart(
                data=trips_per_year_fhv_df,
                x="trip_year",
                y="dispatched_trips",
                x_label="trip_year",
                y_label="dispatched_trips",
                horizontal=False
            )
        else:
            plot_line_single_variable_chart(
                data=trips_per_month_fhv_df,
                column="shared_trips",
                title="Trend of FHV Shared Vehicles - (2020 - Now)",
            )
            st.write("**FHV Shared_Trips Per Year**")
            plot_bar_chart(
                data=trips_per_year_fhv_df,
                x="trip_year",
                y="shared_trips",
                x_label="trip_year",
                y_label="shared_trips",
                horizontal=False
            )
        # --------------------------------------------------------------------------------------------------------------

        time_operating_fhv_df = upload_data(
            key='time_operating',
            direction_str="time_operating_fhv_df",
            extension='parquet',
            caption="Time Operating",
            file_path='./fhvtriptimeoperating/fhvtriptimeoperating.parquet',
            show_data=True
        )
        vehicle_type_fhv_df = upload_data(
            key='vehicle_type',
            direction_str="vehicle_type",
            extension='parquet',
            caption="Vehicle Type",
            file_path='./fhvvehicletype/fhvvehicletype.parquet',
            show_data=True
        )

# ----------------------------------------------------------------------------------------------------------------------

with feature_2:
    create_header("_Quality Of Taxi Services_")

    # ------------------------------------------------------------------------------------------------------------------

    trend_of_dispatch_trips = upload_data(
        key='trend_of_dispatch_trips',
        direction_str="trend_of_dispatch_trips",
        extension='parquet',
        caption="Trend Of Dispatch Trips",
        file_path='./trendofdispatchedtrips/trendofdispatchedtrips.parquet',
        show_data=True
    )
    trend_of_dispatch_trips_option = create_select_box(
        label="Trend Of Dispatch Trips",
        list_elem=[
            "Total Dispatched Trips",
            "Total Dispatched Shared Trips",
            "Unique Dispatched Vehicles"
        ],
        key="trend_of_dispatch_trips_option"
    )
    if trend_of_dispatch_trips_option == "Total Dispatched Trips":
        st.write("**Total Dispatched Trips**")
        plot_bar_chart(
            data=trend_of_dispatch_trips,
            x="year", y="total_dispatched_trips",
            x_label="year",
            y_label="total_dispatched_trips",
            horizontal=False
        )
    elif trend_of_dispatch_trips_option == "Total Dispatched Shared Trips":
        st.write("**Total Dispatched Shared Trips**")
        plot_bar_chart(
            data=trend_of_dispatch_trips,
            x="year", y="total_dispatched_shared_trips",
            x_label="year",
            y_label="total_dispatched_shared_trips",
            horizontal=False
        )
    else:
        st.write("**Unique Dispatched Vehicles**")
        plot_bar_chart(
            data=trend_of_dispatch_trips,
            x="year", y="unique_dispatched_vehicles",
            x_label="year",
            y_label="unique_dispatched_vehicles",
            horizontal=False
        )

    # ------------------------------------------------------------------------------------------------------------------

    version_of_vehicles = upload_data(
        key='version_of_vehicles',
        direction_str="version_of_vehicles",
        extension='parquet',
        caption="Version Of Vehicles",
        file_path='./versionofvehicles/versionofvehicles.parquet',
        show_data=False
    )
    version_of_vehicles_filter = define_fhv_location_filter(
        label="Filter Version Of Vehicles",
        data=version_of_vehicles,
        col_filter="vehicle_year",
    )
    filtered_data_fhv_pickUp = filter_data(
        data=version_of_vehicles,
        referenced_elem=version_of_vehicles.columns[0:1],
        selected_elem=[
            version_of_vehicles_filter
        ],
        caption="Version Of Vehicles",
        show_data=True
    )

    # ------------------------------------------------------------------------------------------------------------------

    reported_vehicle = upload_data(
        key='reported_vehicle',
        direction_str="reported_vehicle",
        extension='parquet',
        caption="Reported Vehicles",
        file_path='./reportedvehicles/reportedvehicles.parquet',
        show_data=True
    )
    reported_base = upload_data(
        key='reported_base',
        direction_str="reported_base",
        extension='parquet',
        caption="Reported Base",
        file_path='./reportedbase/reportedbase.parquet',
        show_data=True
    )

    # ------------------------------------------------------------------------------------------------------------------

    vehicle_activity_option = create_select_box(
        label="Choose FHV Service",
        list_elem=[
            "Black", "Livery",
            "Yellow", "Limousine",
            "Green",  "High Volume",
        ],
        key="vehicle_activity_option"
    )
    if vehicle_activity_option == "Black":
        available_black = upload_data(
            key='available_black',
            direction_str="available_black",
            extension='parquet',
            caption="Available FHV Black ",
            file_path='./availablevehiclesblack/availablevehiclesblack.parquet',
            show_data=False
        )
        on_work_ratio = available_black["on_work_vehicles"] / available_black["total_vehicles"]
        value = on_work_ratio.mean() * 100
        plot_metric(
            label="Available Ratio",
            value=value,
            suffix="%",
        )
        plot_gauge(
            indicator_number=value,
            indicator_color="#0068C9",
            indicator_suffix="%",
            indicator_title="Black Car",
            max_bound=100
        )
    elif vehicle_activity_option == "Livery":
        available_livery = upload_data(
            key='available_livery',
            direction_str="available_livery",
            extension='parquet',
            caption="Available FHV Livery",
            file_path='./availablevehicleslivery/availablevehicleslivery.parquet',
            show_data=False
        )
        on_work_ratio = available_livery["on_work_vehicles"] / available_livery["total_vehicles"]
        value = on_work_ratio.mean() * 100
        plot_metric(
            label="Available Ratio",
            value=value,
            suffix="%",
        )
        plot_gauge(
            indicator_number=value,
            indicator_color="#0068C9",
            indicator_suffix="%",
            indicator_title="Livery Car",
            max_bound=100
        )
    elif vehicle_activity_option == "Yellow":
        available_yellow = upload_data(
            key='available_yellow',
            direction_str="available_yellow",
            extension='parquet',
            caption="Available FHV Yellow",
            file_path='./availablevehiclesyellow/availablevehiclesyellow.parquet',
            show_data=False
        )
        on_work_ratio = available_yellow["on_work_vehicles"] / available_yellow["total_vehicles"]
        value = on_work_ratio.mean() * 100
        plot_metric(
            label="Available Ratio",
            value=value,
            suffix="%",
        )
        plot_gauge(
            indicator_number=value,
            indicator_color="#0068C9",
            indicator_suffix="%",
            indicator_title="Yellow Car",
            max_bound=100
        )
    elif vehicle_activity_option == "Limousine":
        available_limousine = upload_data(
            key='available_limousine',
            direction_str="available_limousine",
            extension='parquet',
            caption="Available FHV Limousine",
            file_path='./availablevehicleslimo/availablevehicleslimo.parquet',
            show_data=False
        )
        on_work_ratio = available_limousine["on_work_vehicles"] / available_limousine["total_vehicles"]
        value = on_work_ratio.mean() * 100
        plot_metric(
            label="Available Ratio",
            value=value,
            suffix="%",
        )
        plot_gauge(
            indicator_number=value,
            indicator_color="#0068C9",
            indicator_suffix="%",
            indicator_title="Limousine",
            max_bound=100
        )
    elif vehicle_activity_option == "Green":
        available_green = upload_data(
            key='available_green',
            direction_str="available_green",
            extension='parquet',
            caption="Available FHV Green",
            file_path='./availablevehiclesgreen/availablevehiclesgreen.parquet',
            show_data=False
        )
        on_work_ratio = available_green["on_work_vehicles"] / available_green["total_vehicles"]
        value = on_work_ratio.mean() * 100
        plot_metric(
            label="Available Ratio",
            value=value,
            suffix="%",
        )
        plot_gauge(
            indicator_number=value,
            indicator_color="#0068C9",
            indicator_suffix="%",
            indicator_title="Green Car",
            max_bound=100
        )
    else:
        available_high_volume = upload_data(
            key='available_high_volume',
            direction_str="available_high_volume",
            extension='parquet',
            caption="Available High Volume",
            file_path='./availablevehicleshighvolume/availablevehicleshighvolume.parquet',
            show_data=False
        )
        on_work_ratio = available_high_volume["on_work_vehicles"] / available_high_volume["total_vehicles"]
        value = on_work_ratio.mean() * 100
        plot_metric(
            label="Available Ratio",
            value=value,
            suffix="%",
        )
        plot_gauge(
            indicator_number=value,
            indicator_color="#0068C9",
            indicator_suffix="%",
            indicator_title="High Volume",
            max_bound=100
        )

    # ------------------------------------------------------------------------------------------------------------------

    if vehicle_activity_option == "Black":
        view_farebox_black = upload_data(
            key='view_farebox_black',
            direction_str="view_farebox_black",
            extension='parquet',
            caption="Farebox FHV Black ",
            file_path='./fareboxpertripsblack/fareboxpertripsblack.parquet',
            show_data=True
        )
        plot_line_single_variable_chart(
            data=view_farebox_black,
            column="farebox_per_day",
            title="Trend Of Service's Black Farebox",
        )
        create_explanation_farebox_details_markdown()
    elif vehicle_activity_option == "Livery":
        view_farebox_livery = upload_data(
            key='view_farebox_livery',
            direction_str="view_farebox_livery",
            extension='parquet',
            caption="Farebox FHV Livery",
            file_path='./fareboxpertripslivery/fareboxpertripslivery.parquet',
            show_data=True
        )
        plot_line_single_variable_chart(
            data=view_farebox_livery,
            column="farebox_per_day",
            title="Trend Of Service's Livery Farebox",
        )
        create_explanation_farebox_details_markdown()
    elif vehicle_activity_option == "Yellow":
        view_farebox_yellow = upload_data(
            key='view_farebox_yellow',
            direction_str="view_farebox_yellow",
            extension='parquet',
            caption="Farebox FHV Yellow",
            file_path='./fareboxpertripsyellow/fareboxpertripsyellow.parquet',
            show_data=True
        )
        plot_line_single_variable_chart(
            data=view_farebox_yellow,
            column="farebox_per_day",
            title="Trend Of Service's Yellow Farebox",
        )
        create_explanation_farebox_details_markdown()
    elif vehicle_activity_option == "Limousine":
        view_farebox_limousine = upload_data(
            key='view_farebox_limousine',
            direction_str="view_farebox_limousine",
            extension='parquet',
            caption="Farebox FHV Limousine",
            file_path='./fareboxpertripslimo/fareboxpertripslimo.parquet',
            show_data=True
        )
        plot_line_single_variable_chart(
            data=view_farebox_limousine,
            column="farebox_per_day",
            title="Trend Of Service's Limousine Farebox",
        )
        create_explanation_farebox_details_markdown()
    elif vehicle_activity_option == "Green":
        view_farebox_green = upload_data(
            key='view_farebox_green',
            direction_str="view_farebox_green",
            extension='parquet',
            caption="Farebox FHV Green",
            file_path='./fareboxpertripsgreen/fareboxpertripsgreen.parquet',
            show_data=True
        )
        plot_line_single_variable_chart(
            data=view_farebox_green,
            column="farebox_per_day",
            title="Trend Of Service's Green Farebox",
        )
        create_explanation_farebox_details_markdown()
    else:
        view_farebox_high_volume = upload_data(
            key='view_farebox_high_volume',
            direction_str="view_farebox_high_volume",
            extension='parquet',
            caption="Farebox High Volume",
            file_path='./fareboxpertripshighvolume/fareboxpertripshighvolume.parquet',
            show_data=True
        )
        plot_line_single_variable_chart(
            data=view_farebox_high_volume,
            column="farebox_per_day",
            title="Trend Of Service's High Volume Farebox",
        )
        create_explanation_farebox_details_markdown()
