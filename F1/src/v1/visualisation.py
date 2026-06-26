# FastF1 Deps
from fastf1.core import Session

from fastf1.plotting import get_driver_color_mapping

# Data Deps
from pandas import DataFrame

# Visualisation Deps
import matplotlib.pyplot as plt
from matplotlib import figure
import seaborn as sns

# Source Deps
from src.v1.config import (
    AERO_VIS_CONFIG,
    ERS_VIS_CONFIG,
    KE_VIS_CONFIG,
    POWER_VIS_CONFIG
)


class DataVisualisation:
    """This class is responsible for all the Visualisation generated on the data
    that was processed using the downstream API."""

    def __init__(
        self, 
        session: Session,
        driver_names: list,
    ) -> None:
        
        # Accessing all the Driver Colors
        driver_colors_full = get_driver_color_mapping(session=session)
        
        # Cacheing the Necessary Driver Colors
        self.driver_color = {}
        for driver in driver_names:
            self.driver_color[driver] = driver_colors_full[driver]

    # ======================= Member Methods =======================
    def create_scatter_plots(
        self,
        nrows: int,
        ncols: int,
        figsize: tuple[int, int],
        data: DataFrame,
        hue: str,
        size: int,
        plot_kind: str,
    ) -> figure.Figure:
        """This function creates a Matplotlib canvas of subplots and creates a Seaborn
        Scatterplot for each of the subplots. It then returns the completed figure."""

        # Matplotlib Canvas
        fig, axes = plt.subplots(
            nrows=nrows,
            ncols=ncols,
            figsize=figsize,
            sharex=False,
            sharey=False
        )

        # Flatten the axes only when multiple subplots are being used
        if nrows > 1 or ncols > 1:
            axes = axes.flatten()

        # Accessing the Scatter Plot Configurations
        plot_configs = None
        if plot_kind == "aero":
            plot_configs = AERO_VIS_CONFIG
        elif plot_kind == "ers_clip":
            plot_configs = ERS_VIS_CONFIG

        # Sanity Check for Plot Configs
        assert plot_configs, "The plot configuration wasn't satisfied."

        # If there is only one subplot
        if len(plot_configs) == 1:
            x, y, title = plot_configs[0]

            # Subplot for the Axes.
            sns.scatterplot(
                data=data,
                x=x,
                y=y,
                hue=hue,
                palette=self.driver_color,
                ax=axes,
                s=size
            )
            axes.set_title(title, pad=25)
            axes.grid()
        # If there are multiple subplots
        else:
            for ax_idx, ax_config in enumerate(plot_configs):
                x, y, title = ax_config

                # Subplot for the Axes.
                sns.scatterplot(
                    data=data,
                    x=x,
                    y=y,
                    hue=hue,
                    palette=self.driver_color,
                    ax=axes[ax_idx],
                    s=size
                )
                axes[ax_idx].set_title(title, pad=25)
                axes[ax_idx].grid()
            
        return fig
    
    def create_bar_plots(
            self,
            nrows: int,
            ncols: int,
            figsize: tuple[int, int],
            data: DataFrame,
            hue: str,
            plot_kind: str
        ) -> figure.Figure:
        """This function creates a Matplotlib canvas of subplots and creates a Seaborn
        Barplot for each of the subplots. It then returns the completed figure."""

        # Matplotlib Canvas
        fig, axes = plt.subplots(
            nrows=nrows,
            ncols=ncols,
            sharex=False,
            sharey=False,
            figsize=figsize
        )
        axes = axes.flatten()

        # Accessing the Scatter Plot Configurations
        plot_configs = None
        if plot_kind == "ke":
            plot_configs = KE_VIS_CONFIG
        elif plot_kind == "power":
            plot_configs = POWER_VIS_CONFIG

        # Sanity Check for Plot Configs
        assert plot_configs, "The plot configuration wasn't satisfied."
        for ax_idx, ax_config in enumerate(plot_configs):
            x, y, title = ax_config

            # Subplot for the Axes.
            sns.barplot(
                data=data,
                x=x,
                y=y,
                hue=hue,
                palette=self.driver_color,
                ax=axes[ax_idx],
            )
            
            axes[ax_idx].set_title(title, pad=25)
            axes[ax_idx].grid()

        return fig