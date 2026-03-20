"""
Dash可视化仪表盘

提供交互式四维矩阵可视化
"""

from typing import Dict, Any, Optional
import json

import dash
from dash import dcc, html, Input, Output, callback
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import numpy as np

from hypercube.core.hypercube import HyperCube


def create_hypercube_dashboard(hypercube: HyperCube, port: int = 8050):
    """
    创建四维矩阵可视化仪表盘
    
    Args:
        hypercube: 超立方体实例
        port: 服务端口
    """
    app = dash.Dash(__name__, title="四维矩阵数据库可视化")
    
    # 准备数据
    viz_data = hypercube.export_for_visualization()
    
    # 提取分类信息
    z_categories = viz_data["categories"]["z"]
    x_stages = viz_data["categories"]["x"]
    
    # 构建布局
    app.layout = html.Div([
        html.H1("四维矩阵数据库可视化", style={"textAlign": "center"}),
        
        # 控制面板
        html.Div([
            html.Div([
                html.Label("主题分类 (Z轴):"),
                dcc.Dropdown(
                    id="z-selector",
                    options=[
                        {"label": f"{v} (ID: {k})", "value": k}
                        for k, v in z_categories.items()
                    ],
                    value=list(z_categories.keys())[0] if z_categories else None,
                    multi=True,
                ),
            ], style={"width": "30%", "display": "inline-block", "padding": "10px"}),
            
            html.Div([
                html.Label("时间维度 (T轴):"),
                dcc.Slider(
                    id="t-slider",
                    min=0,
                    max=10,
                    step=1,
                    value=0,
                    marks={i: str(i) for i in range(11)},
                ),
            ], style={"width": "40%", "display": "inline-block", "padding": "10px"}),
            
            html.Div([
                html.Label("视图模式:"),
                dcc.RadioItems(
                    id="view-mode",
                    options=[
                        {"label": "XY平面", "value": "xy"},
                        {"label": "XZ平面", "value": "xz"},
                        {"label": "YZ平面", "value": "yz"},
                        {"label": "3D散点", "value": "3d"},
                    ],
                    value="xy",
                ),
            ], style={"width": "25%", "display": "inline-block", "padding": "10px"}),
        ], style={"backgroundColor": "#f0f0f0", "padding": "20px"}),
        
        # 统计信息
        html.Div(id="stats-panel", style={"padding": "20px"}),
        
        # 主可视化区域
        dcc.Graph(id="main-visualization", style={"height": "600px"}),
        
        # 颜色趋势
        html.H3("颜色流动趋势", style={"textAlign": "center"}),
        dcc.Graph(id="color-trend", style={"height": "300px"}),
        
        # 详情面板
        html.Div(id="detail-panel", style={"padding": "20px", "backgroundColor": "#fafafa"}),
        
    ])
    
    @callback(
        Output("main-visualization", "figure"),
        Output("stats-panel", "children"),
        Input("z-selector", "value"),
        Input("t-slider", "value"),
        Input("view-mode", "value"),
    )
    def update_visualization(selected_z, t_value, view_mode):
        if not selected_z:
            selected_z = [list(z_categories.keys())[0]] if z_categories else [0]
        
        if not isinstance(selected_z, list):
            selected_z = [selected_z]
        
        # 过滤数据点
        filtered_points = [
            p for p in viz_data["data_points"]
            if p["coordinates"]["z"] in selected_z
        ]
        
        if not filtered_points:
            return go.Figure(), html.Div("无数据")
        
        # 统计信息
        stats = html.Div([
            html.Span(f"数据点: {len(filtered_points)} | ", style={"marginRight": "20px"}),
            html.Span(f"总表数: {len(set(p['data']['table_name'] for p in filtered_points))} | ",
                     style={"marginRight": "20px"}),
            html.Span(f"总行数: {sum(p['data']['row_count'] for p in filtered_points):,}"),
        ])
        
        # 创建图表
        if view_mode == "xy":
            fig = create_xy_scatter(filtered_points, z_categories)
        elif view_mode == "xz":
            fig = create_xz_scatter(filtered_points, z_categories)
        elif view_mode == "yz":
            fig = create_yz_scatter(filtered_points, z_categories)
        else:  # 3d
            fig = create_3d_scatter(filtered_points, z_categories)
        
        return fig, stats
    
    @callback(
        Output("color-trend", "figure"),
        Input("z-selector", "value"),
    )
    def update_color_trend(selected_z):
        if not selected_z:
            return go.Figure()
        
        if not isinstance(selected_z, list):
            selected_z = [selected_z]
        
        fig = go.Figure()
        
        for z in selected_z:
            flow = hypercube.get_color_flow(z)
            if flow:
                times = [f["time"] for f in flow]
                colors = [f["color"] for f in flow]
                
                fig.add_trace(go.Scatter(
                    x=list(range(len(times))),
                    y=[z] * len(times),
                    mode="markers",
                    marker=dict(
                        size=20,
                        color=colors,
                    ),
                    name=z_categories.get(z, f"Z={z}"),
                ))
        
        fig.update_layout(
            title="主题颜色时间流动",
            xaxis_title="时间",
            yaxis_title="主题分类",
            showlegend=True,
        )
        
        return fig
    
    @callback(
        Output("detail-panel", "children"),
        Input("main-visualization", "clickData"),
    )
    def update_detail(click_data):
        if not click_data:
            return html.Div("点击数据点查看详情")
        
        point = click_data["points"][0]
        customdata = point.get("customdata", {})
        
        return html.Div([
            html.H4("数据详情"),
            html.Pre(json.dumps(customdata, indent=2, ensure_ascii=False)),
        ])
    
    return app


def create_xy_scatter(points: list, z_categories: dict) -> go.Figure:
    """创建XY平面散点图"""
    fig = go.Figure()
    
    # 按z分组
    z_groups = {}
    for p in points:
        z = p["coordinates"]["z"]
        if z not in z_groups:
            z_groups[z] = []
        z_groups[z].append(p)
    
    for z, group_points in z_groups.items():
        x_vals = [p["coordinates"]["x"] for p in group_points]
        y_vals = [p["coordinates"]["y"] for p in group_points]
        colors = [p["color"]["hex"] for p in group_points]
        texts = [p["data"]["table_name"] for p in group_points]
        customdata = [p["data"] for p in group_points]
        
        fig.add_trace(go.Scatter(
            x=x_vals,
            y=y_vals,
            mode="markers",
            marker=dict(
                size=15,
                color=colors,
                line=dict(width=1, color="black"),
            ),
            text=texts,
            hovertemplate="<b>%{text}</b><br>X: %{x}<br>Y: %{y}<extra></extra>",
            customdata=customdata,
            name=z_categories.get(z, f"Z={z}"),
        ))
    
    fig.update_layout(
        title="XY平面视图 (X:业务阶段, Y:量级)",
        xaxis_title="业务阶段 (X)",
        yaxis_title="量级 (Y, 对数压缩)",
        hovermode="closest",
    )
    
    return fig


def create_xz_scatter(points: list, z_categories: dict) -> go.Figure:
    """创建XZ平面散点图"""
    fig = go.Figure()
    
    x_vals = [p["coordinates"]["x"] for p in points]
    z_vals = [p["coordinates"]["z"] for p in points]
    colors = [p["color"]["hex"] for p in points]
    texts = [p["data"]["table_name"] for p in points]
    
    fig.add_trace(go.Scatter(
        x=x_vals,
        y=z_vals,
        mode="markers",
        marker=dict(
            size=15,
            color=colors,
            line=dict(width=1, color="black"),
        ),
        text=texts,
        hovertemplate="<b>%{text}</b><br>X: %{x}<br>Z: %{y}<extra></extra>",
    ))
    
    fig.update_layout(
        title="XZ平面视图 (X:业务阶段, Z:主题分类)",
        xaxis_title="业务阶段 (X)",
        yaxis_title="主题分类 (Z)",
        yaxis=dict(
            ticktext=list(z_categories.values()),
            tickvals=list(z_categories.keys()),
        ),
    )
    
    return fig


def create_yz_scatter(points: list, z_categories: dict) -> go.Figure:
    """创建YZ平面散点图"""
    fig = go.Figure()
    
    y_vals = [p["coordinates"]["y"] for p in points]
    z_vals = [p["coordinates"]["z"] for p in points]
    colors = [p["color"]["hex"] for p in points]
    texts = [p["data"]["table_name"] for p in points]
    
    fig.add_trace(go.Scatter(
        x=y_vals,
        y=z_vals,
        mode="markers",
        marker=dict(
            size=15,
            color=colors,
            line=dict(width=1, color="black"),
        ),
        text=texts,
        hovertemplate="<b>%{text}</b><br>Y: %{x}<br>Z: %{y}<extra></extra>",
    ))
    
    fig.update_layout(
        title="YZ平面视图 (Y:量级, Z:主题分类)",
        xaxis_title="量级 (Y, 对数压缩)",
        yaxis_title="主题分类 (Z)",
        yaxis=dict(
            ticktext=list(z_categories.values()),
            tickvals=list(z_categories.keys()),
        ),
    )
    
    return fig


def create_3d_scatter(points: list, z_categories: dict) -> go.Figure:
    """创建3D散点图"""
    x_vals = [p["coordinates"]["x"] for p in points]
    y_vals = [p["coordinates"]["y"] for p in points]
    z_vals = [p["coordinates"]["z"] for p in points]
    colors = [p["color"]["hex"] for p in points]
    texts = [p["data"]["table_name"] for p in points]
    
    fig = go.Figure(data=[go.Scatter3d(
        x=x_vals,
        y=y_vals,
        z=z_vals,
        mode="markers",
        marker=dict(
            size=6,
            color=colors,
        ),
        text=texts,
        hovertemplate="<b>%{text}</b><br>X: %{x}<br>Y: %{y}<br>Z: %{z}<extra></extra>",
    )])
    
    fig.update_layout(
        title="3D视图 (X:业务阶段, Y:量级, Z:主题分类)",
        scene=dict(
            xaxis_title="业务阶段 (X)",
            yaxis_title="量级 (Y)",
            zaxis_title="主题分类 (Z)",
        ),
    )
    
    return fig
