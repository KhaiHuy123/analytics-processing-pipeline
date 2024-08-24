
import random
import pandas as pd
import polars as pl
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import matplotlib.pyplot as plt
import duckdb


def create_logo(image_path):
    st.sidebar.image(image_path, caption="")


def create_data_expander(df, caption='Data Preview', use_container_width=True, expanded=False):
    with st.expander(caption, expanded):
        st.dataframe(df, use_container_width=use_container_width) if use_container_width else st.dataframe(df)


def create_header(header):
    st.sidebar.header(f":blue[{header}]")


def create_sub_header(header, divider):
    st.subheader(header, divider=divider)


def create_select_box(label, list_elem, key: str, index=0):
    option = st.sidebar.selectbox(label=label, options=[elem for elem in list_elem], index=index, key=key)
    return option


def setup_location_filter(title):
    st.sidebar.header(title)


def setup_css_style(css):
    with open(f"{css}") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)


def upload_file(direction_str='Upload a file through config', extension="parquet", key='index', cation="Data Preview",
                file_path=None, show_data=True):
    if file_path is None:
        uploaded_file = st.sidebar.file_uploader(label="Choose a file", key=key)

        if uploaded_file is None:
            st.info(direction_str)
            st.stop()

        df = load_data(uploaded_file, f_type=extension)
        if show_data:
            create_data_expander(df, caption=cation)
    else:
        df = load_data(file_path, f_type=extension)
        if show_data:
            create_data_expander(df, caption=cation)
    return df


def upload_data(key, direction_str: str, extension="parquet", caption="Data Preview", file_path=None, show_data=True):
    data = upload_file(direction_str=direction_str, extension=extension, key=key, file_path=file_path,
                       show_data=show_data, cation=caption)
    return data


def setup_config(page_title="my_title", page_icon=":bar_chart:", title=None, markdown=None):
    st.set_page_config(page_title=page_title, page_icon=page_icon, layout='wide')
    st.sidebar.title(title) if title is not None\
        else st.title(":blue[WELCOME TO MY DATA SHOW] :sunglasses:")
    st.sidebar.markdown(markdown) if markdown is not None \
        else st.markdown("_Prototype v0.0.1_")


def select_data(data: pd.DataFrame, referenced_elem: list, selected_elem: list):
    if len(referenced_elem) != len(selected_elem):
        raise ValueError("Two iterations nees to have the same length")

    query_parts = [f"{ref} == {sel}" for ref, sel in zip(referenced_elem, selected_elem)]
    query_string = " & ".join(query_parts)
    data_selected = data.query(query_string)
    return data_selected


def select_data_using_sql_statement(data: pd.DataFrame, col_name, threshold=100):
    data_selected = duckdb.query(f"""
        SELECT * FROM data
        where {col_name} >= {threshold}
    """).df()
    return data_selected


def define_gy_distance_moving_filter(label, data: pd.DataFrame, col_filter, short_options=True):
    options = data[col_filter].unique()
    pick_up = st.sidebar.multiselect(label=label, options=options, default=options[0:10] if short_options else options)
    return pick_up


def define_filter(label, data: pd.DataFrame, col_filter: str):
    options = data[col_filter].unique()
    pick_up = st.sidebar.multiselect(label=label, options=options, default=options)
    return pick_up


def filter_data(data, referenced_elem: list, selected_elem: list, use_container_width=True,
                show_data=True, caption="Data Preview"):
    result = select_data(data=data, referenced_elem=referenced_elem, selected_elem=selected_elem)
    if show_data:
        create_data_expander(result, use_container_width=use_container_width, caption=caption)
    return result


def create_borough_markdown():
    st.markdown("""
        New York City consists of five boroughs, each with its own distinct character and government structure:
        
        Manhattan:
        Known as the financial, commercial, and cultural center of the city, 
        Manhattan is home to iconic landmarks such as Central Park, Times Square, and Wall Street. \n
        Brooklyn:
        The most populous borough, Brooklyn is known for its diverse neighborhoods, vibrant arts scene, 
        and historic landmarks such as the Brooklyn Bridge and Coney Island. \n
        Queens:
        Queens is the largest borough by area and is one of the most 
        ethnically diverse urban areas in the world.
        It is known for its diverse communities, cultural institutions, 
        and attractions like Flushing Meadows-Corona Park and JFK Airport. \n
        Bronx: 
        Located north of Manhattan, the Bronx is known for its rich cultural heritage, 
        including the birthplace of hip-hop and the Bronx Zoo. 
        It is also home to the New York Botanical Garden and Yankee Stadium. \n
        Staten Island:
        Situated to the southwest of Manhattan, 
        Staten Island is the least densely populated borough and is known for its suburban feel, parks,
        and waterfront attractions like the Staten Island Ferry and Snug Harbor Cultural Center.
        """)


def create_zone_markdown():
    st.markdown("""
        NYC Taxi Zones, which correspond to the pickup and drop-off zones, or LocationIDs,
        included in the Yellow, Green, and FHV Trip Records published to Open Data. 
        """)


def create_zone_details_markdown():
    st.markdown("""
        _According to TLC There are 265 Taxi-Zone. The taxi zones are roughly based on 
        NYC Department of City Planning’s Neighborhood Tabulation Areas (NTAs) 
        and are meant to approximate neighborhoods, so you can see which neighborhood a passenger was picked up in, 
        and which neighborhood they were dropped off in._
        """)


def create_explanation_distance_moving_details_markdown():
    st.markdown("""
        _We can see two trend included in/out zone trips and long/shot distance trips._
        
        _Each Taxi Zone represents to one location. Each location is bound to one ID._ 
        
        _Trend of long/short distance-moving trips is determined by using line plot for all records in dataset._
        
        _"Index" label represents to collected records of taxi-trips provided by Open Data ( view data above )._
        
        _Long distance trips are the trips that have distance moving average larger or equal than 100.
         Short distance is the reverse case_
        
        """)


def create_explanation_farebox_details_markdown():
    st.markdown("""
        _"Index" label represents to 1 month. Timeline is determined from 2020 to present._
        _We can see the trend of data is fallen down sightly. It is because of Covid-19 pandemic's affect._
        _( view data above )_
    """)


@st.cache_data
def load_data(path: str, f_type="parquet"):
    if f_type == 'parquet':
        data = pl.read_parquet(path)
    elif f_type == 'csv':
        data = pl.read_csv(path)
    else:
        data = pl.read_json(path)
    return data.to_pandas()


@st.cache_data
def plot_histogram(data, column, bins=10, title=None, x_label=None, y_label=None, plot_type="plotly"):
    if plot_type == "plotly":
        fig = px.histogram(data, x=column, nbins=bins, title=title, labels={'x': x_label, 'y': y_label}) \
            if title is not None else px.histogram(data, x=column, nbins=bins, labels={'x': x_label, 'y': y_label})
        st.plotly_chart(fig)
    elif plot_type == "matplotlib":
        plt.hist(data[column], bins=bins)
        if x_label is not None:
            plt.ylabel(x_label)
        if y_label is not None:
            plt.ylabel(y_label)
        if title is not None:
            plt.title(title)
        st.pyplot()
    else:
        st.error("Please Choose 'plotly' Or 'matplotlib'.")


@st.cache_data
def plot_line_single_variable_chart(data, column, x_label=None, y_label=None, title=None, plot_type="plotly"):
    if plot_type == "plotly":
        fig = px.line(data, y=column, title=title, labels={'x_label': x_label, 'y_label': y_label}) \
            if title is not None or x_label is not None or y_label is not None else\
            px.histogram(data, y=column)
        st.plotly_chart(fig)
    elif plot_type == "matplotlib":
        plt.plot(data[column])
        if x_label is not None:
            plt.ylabel(x_label)
        if y_label is not None:
            plt.ylabel(y_label)
        if title is not None:
            plt.title(title)
        st.pyplot(plt)
    else:
        st.error("Please Choose 'plotly' Or 'matplotlib'.")


@st.cache_data
def plot_custom_line_chart(data, x, y, color=None, text=None, title=None, markers=True):
    fig = px.line(data_frame=data, x=x, y=y, color=color, text=text, markers=markers, title=title)\
        if color is not None or text is not None or title is not None \
        else px.line(data_frame=data, x=x, y=y, markers=markers)
    st.plotly_chart(fig, use_container_width=True)


@st.cache_data
def plot_custom_bar_chart(data, x, y, color, title):
    fig = px.bar(data_frame=data, x=x, y=y, color=color, title=title)
    st.plotly_chart(fig, use_container_width=True)


@st.cache_data
def plot_line_chart(data, x, y, x_label, y_label, color=None):
    if color is None:
        st.line_chart(data=data, x=x, y=y, x_label=x_label, y_label=y_label)
    else:
        st.line_chart(data=data, x=x, y=y, x_label=x_label, y_label=y_label, color=color)


@st.cache_data
def plot_bar_chart(data, x, y, x_label, y_label, horizontal=True):
    if horizontal:
        st.bar_chart(data=data, x=x, y=y, x_label=x_label, y_label=y_label, horizontal=horizontal)
    else:
        st.bar_chart(data=data, x=x, y=y, x_label=x_label, y_label=y_label)


@st.cache_data
def plot_metric(label, value, prefix="", suffix="", show_graph=False, color_graph=""):
    fig = go.Figure()

    fig.add_trace(
        go.Indicator(value=value, gauge={"axis": {"visible": False}},
                     number={"prefix": prefix, "suffix": suffix, "font.size": 28},
                     title={"text": label, "font": {"size": 24}}))
    if show_graph:
        fig.add_trace(go.Scatter(y=random.sample(range(0, 101), 30), hoverinfo="skip", fill="tozeroy",
                                 fillcolor=color_graph, line={"color": color_graph}))
    fig.update_xaxes(visible=False, fixedrange=True)
    fig.update_yaxes(visible=False, fixedrange=True)
    fig.update_layout(margin=dict(t=30, b=0), showlegend=False, plot_bgcolor="light green", height=100)
    st.plotly_chart(fig, use_container_width=True)


@st.cache_data
def plot_gauge(indicator_number, indicator_color, indicator_suffix, indicator_title, max_bound):
    fig = go.Figure(
        go.Indicator(value=indicator_number, mode="gauge+number",
                     domain={"x": [0, 1], "y": [0, 1]},
                     number={"suffix": indicator_suffix, "font.size": 26},
                     gauge={"axis": {"range": [0, max_bound], "tickwidth": 1}, "bar": {"color": indicator_color}},
                     title={"text": indicator_title, "font": {"size": 25}}))
    fig.update_layout(height=200, margin=dict(l=10, r=10, t=50, b=10, pad=8))
    st.plotly_chart(fig, use_container_width=True)
