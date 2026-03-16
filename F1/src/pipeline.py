from pandas import DataFrame, cut

from src.utils import (
    TELEMETRY_KEYPOINTS_BY_DIST,
    AVG_LOAD_KEYS,
    MAX_LOAD_KEYS
)


class Pipeline:
    """This class implements the full end-to-end data pipelines that will be
    used by the notebooks and dashboard for processing data from the race-weekend."""

    # ============ Pipeline Class Vars for Fuel Calculations ============
    # Max 110kg by regulations but not filled to the max for race time.
    init_fuel_load = 95

    # Fuel Penality confirmed by LH44: 10kgs => 0.3sec
    fuel_time_penality_per_kg = 0.3 / 10

    # Relaxation Factor for the Race fuel consumption
    relaxation_factor = (0.06, 0.04, 0.02)

    # Fuel burn upper limit during a race 100kg/hr
    fuel_flow_rate_per_kg = 100 / 3600
    fuel_flow_constraint = (1, 0.5, 0.2)

    # ============ Standard Methods ============
    def __init__(self) -> None:
        pass

    # ============ Member Methods ============
    def map_telemetry_keypoints(self, copy_frame: DataFrame) -> DataFrame:
        """Performs the mapping between the Telemetry Distance channel and
        identified keypoints and returns the modified copy of the dataframe.
        
        Args:
        - copy_frame: pd.DataFrame
        
        Returns:
        - copy_frame: pd.DataFrame"""

        lap_reset_offset = 4228.4594
        
        # Offset the cumulative distance measure wrt each lap
        copy_frame["Distance"] = copy_frame["Distance"].apply(
            lambda x: (
                x if x <= lap_reset_offset 
                else ((x / lap_reset_offset) - (x // lap_reset_offset)) * lap_reset_offset
            )
        )

        # Binning the telemetry keypoint based on distance
        copy_frame["Keypoint"] = cut(
            x=copy_frame["Distance"],
            right=False,
            labels=list(TELEMETRY_KEYPOINTS_BY_DIST.keys()),
            bins=list(TELEMETRY_KEYPOINTS_BY_DIST.values()) + [lap_reset_offset]
        )

        return copy_frame
    
    def get_fuel_burn(
            self,
            laptime: float,
            driver_throttle_handle: tuple[float, float, float]
        ) -> float:
        """Performs the calculations to estimate the fuel burnt over a single lap using the flying
        lap from Q1 as the closest estimate on pure pace to a regular race lap.
        
        Args:
        - laptime: float
        - driver_throttle_handle: tuple[float, float, float]
        
        Returns:
        - lap_fuel_burn: float"""

        lap_fuel_burn = (
            # Full Throttle Consumption
            (Pipeline.fuel_flow_rate_per_kg * Pipeline.fuel_flow_constraint[0] * laptime * (driver_throttle_handle[0] - Pipeline.relaxation_factor[0]))
            # Throttle Transition Consumption
            + (Pipeline.fuel_flow_rate_per_kg * Pipeline.fuel_flow_constraint[1] * laptime * (driver_throttle_handle[1] + Pipeline.relaxation_factor[1]))
            # LiCo Throttle Consumption
            + (Pipeline.fuel_flow_rate_per_kg * Pipeline.fuel_flow_constraint[2] * laptime * (driver_throttle_handle[2] + Pipeline.relaxation_factor[2]))
        )

        return lap_fuel_burn
    
    def get_fuel_aware_laptime(
            self,
            driver_laps: DataFrame,
            driver_throttle_handle: tuple
        ) -> DataFrame:
        """Performs the calculations to transform the driver_laps laptimes
        to be corrected for the reducing fuel load during a race. It also 
        adds additional fuel related features and returns the modified driver_laps.
        
        New Features:
        - LapFuelBurn: The estimated fuel burnt during a single lap based on each driver's throttle map
        - CumulativeLapFuelBurn: The cumulative fuel load burnt with each lap till the end of the race
        - FuelAwareLapTime: The fuel load corrected laptime

        Args:
        - driver_laps: pd.DataFrame
        - driver_throttle_handle: tuple
        
        Returns:
        - driver_laps: pd.DataFrame"""
        
        # Copying the Race Laptimes and Converting them to Seconds
        driver_laptimes = driver_laps["LapTime"].copy()
        driver_laptimes = driver_laptimes.dt.total_seconds()

        # Calculating the Lap-wise Fuel Burn for each driver
        driver_laps["LapFuelBurn"] = driver_laptimes.apply(
            lambda x: self.get_fuel_burn(
                laptime=x,
                driver_throttle_handle=driver_throttle_handle
            )
        )

        # Calculating the Cumulative Fuel Burn for each driver over the race
        driver_laps["CumulativeFuelBurn"] = driver_laps["LapFuelBurn"].cumsum()

        # Applying the fuel penality offset to correct the laptimes over the race
        driver_laps["FuelAwareLapTime"] = (
            driver_laptimes - 
            (Pipeline.init_fuel_load - driver_laps["CumulativeFuelBurn"]) * Pipeline.fuel_time_penality_per_kg
        )

        return driver_laps
    
    def get_efficiency_index_corner_to_straight(
        self, 
        driver_point_estimates_by_trace: DataFrame
    ) -> tuple[float, float]:
        """Calculates the Efficiency Index (Corner V Straight) for each driver based on the telemetry 
        from Q1. The Efficiency Index (C_S) is defined as the ratio of mean to max speeds during cornering and 
        traction keypoints around the track.
        
        Args:
        - driver_point_estimates_by_trace: pd.DataFrame

        Returns:
        - (avg_eff, stl_eff): tuple[float, float]"""
        
        # Avg Load Efficiency: The ratio of mean speed to max speed through cornering keypoints
        # Corner Entry Load,  Cornering Load, Corner Exit Load 
        avg_load_keypoints = driver_point_estimates_by_trace.loc[AVG_LOAD_KEYS].copy()
        avg_load_keypoints["per_corner_efficiency"] = avg_load_keypoints["mean"] / avg_load_keypoints["max"]

        # Max Load Efficiency: The ratio of mean speed to max speed through traction keypoints
        max_load_keypoints = driver_point_estimates_by_trace.loc[MAX_LOAD_KEYS].copy()
        max_load_keypoints["per_stl_efficiency"] = max_load_keypoints["mean"] / max_load_keypoints["max"]

        # Cornering Efficiency wrt Max
        avg_eff = avg_load_keypoints["per_corner_efficiency"].mean()

        # Straight Line Efficiency wrt Max
        stl_eff = max_load_keypoints["per_stl_efficiency"].mean()

        return avg_eff, stl_eff
