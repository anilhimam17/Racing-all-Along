from fastf1.core import Telemetry

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from src.utils import DRIVER_NUMBER_MAP


class Plotting:
    """This class implements helper functions for generating visualisations
    of the data that has been loaded from a race-weekend."""

    def __init__(self) -> None:
        pass

    def plot_driver_telemetry_traces(
            self,
            top_5_driver_telemetry: dict[str, Telemetry],
            lap_number: int = 0,
            show_distance: bool = False
        ) -> None:
        """Utilises Plotly to generate telemetry traces for the drivers to 
        compare driving, braking and throttle styles."""

        fig = make_subplots(
            rows=5,
            shared_xaxes=False,
            shared_yaxes=False,
            vertical_spacing=0.1,
            figure=go.Figure(layout=go.Layout(height=1920, width=1540)),
            subplot_titles=[
                f"{DRIVER_NUMBER_MAP[driver_number]}" 
                for driver_number in top_5_driver_telemetry
            ]
        )

        top_5_drivers = list(top_5_driver_telemetry.keys())

        # Iterating through the top 5 drivers to generate the subplot
        for idx, driver_number in enumerate(top_5_drivers):

            # Accessing the specific driver's telemetry
            driver_telemetry = top_5_driver_telemetry[driver_number]

            # Rescaling the values of key traces being visualised
            rpm_plot = (driver_telemetry["RPM"] / driver_telemetry["RPM"].max()) * 100
            speed_plot = (driver_telemetry["Speed"] / driver_telemetry["Speed"].max()) * 100
            gear_plot = (driver_telemetry["nGear"] / driver_telemetry["nGear"].max()) * 100
            brake_plot = (driver_telemetry["Brake"].map({True: 1, False: 0})) * 100
            dist_plot = (driver_telemetry["Distance"] / driver_telemetry["Distance"].max()) * 100

            # Generating the Distance Trace
            if show_distance:
                fig.add_trace(
                    go.Scatter(
                        x=driver_telemetry["Date"].dt.time,
                        y=dist_plot,
                        name="Distance",
                        hovertemplate="%{text}<extra></extra>",
                        text=[f"Distance: {round(driver_telemetry.iloc[i]["Distance"], 3)}" for i in range(len(driver_telemetry))],
                        legendgroup="Distance",
                        legendgrouptitle_text="Distance Traces"
                    ),
                    row=idx + 1,
                    col=1
                )

            # Generating the Throttle Trace
            fig.add_trace(
                go.Scatter(
                    x=driver_telemetry["Date"].dt.time,
                    y=driver_telemetry["Throttle"],
                    name="Throttle",
                    hovertemplate="Throttle: %{y}<extra></extra>",
                    legendgroup="Throttle",
                    legendgrouptitle_text="Throttle Traces"
                ),
                row=idx + 1,
                col=1
            )

            # Generating the RPM Trace
            fig.add_trace(
                go.Scatter(
                    x=driver_telemetry["Date"].dt.time,
                    y=rpm_plot,
                    name="RPM",
                    hovertemplate="%{text}<extra></extra>",
                    text=[f"RPM: {driver_telemetry.iloc[i]['RPM']}" for i in range(len(driver_telemetry))],
                    legendgroup="RPM",
                    legendgrouptitle_text="RPM Traces"
                ),
                row=idx + 1,
                col=1
            )

            # Generating the Speed Trace
            fig.add_trace(
                go.Scatter(
                    x=driver_telemetry["Date"].dt.time,
                    y=speed_plot,
                    name="Speed",
                    hovertemplate="%{text}<extra></extra>",
                    text=[f"Speed: {driver_telemetry.iloc[i]['Speed']}" for i in range(len(driver_telemetry))],
                    legendgroup="Speed",
                    legendgrouptitle_text="Speed Traces"
                ),
                row=idx + 1,
                col=1
            )

            # Generating the Gear Trace
            fig.add_trace(
                go.Scatter(
                    x=driver_telemetry["Date"].dt.time,
                    y=gear_plot,
                    name="Gear",
                    hovertemplate="%{text}<extra></extra>",
                    text=[f"Gear: {driver_telemetry.iloc[i]['nGear']}" for i in range(len(driver_telemetry))],
                    legendgroup="Gear",
                    legendgrouptitle_text="Gear Shifts"
                ),
                row=idx + 1,
                col=1
            )

            # Generating the Brake Trace
            fig.add_trace(
                go.Scatter(
                    x=driver_telemetry["Date"].dt.time,
                    y=brake_plot,
                    name="Brake",
                    hovertemplate="Brake: %{y}<extra></extra>",
                    legendgroup="Braking",
                    legendgrouptitle_text="Braking Traces"
                ),
                row=idx + 1,
                col=1
            )

        fig.update_layout(hovermode="x unified")
        fig.show()
