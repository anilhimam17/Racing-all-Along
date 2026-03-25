from pandas import DataFrame, cut

from fastf1.core import Telemetry

from src.utils import (
    TELEMETRY_KEYPOINTS_BY_DIST,
    AVG_LOAD_KEYS,
    MAX_LOAD_KEYS,
    BRAKING_KEYS
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

    # ============ Pipeline Class Vars for Force and Energy calculations ============

    # Average Mass of the Car with Fuel Load
    car_mass = 800

    # Average Mass of the Driver
    driver_mass = 75

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
    
    def get_keypoint_traction_energy(
            self, 
            driver_telemetry_trace: Telemetry
        ) -> dict[str, tuple[float, float]]:
        """Calculates the Traction Energy that is used by each driver based on the telemetry 
        from Q1 for each mini-sector. The Traction Energy is defined as the energy withdrawn 
        from the tyre for every meter of a lap by mapping the interactions between 
        Throttle, Speed and Relative Distance travelled.
        
        Args:
        - driver_telemetry_trace: Telemetry
        
        Returns:
        - driver_keypoint_digest: dict[str, tuple[float, float]]"""

        driver_keypoint_digest = {}
        
        # Iterating through the grouped keypoints sample from the driver telemetry
        grouped_keypoint_telemetry = driver_telemetry_trace.groupby("Keypoint", observed=False)
        for keypoint_group, telemetry in grouped_keypoint_telemetry:
        
            # Raw Unscaled Traction Energy: Weights Corners and Straight equally within each mini-sector
            raw_traction_energy = (
                telemetry["Speed"] * 
                telemetry["Throttle"] * 
                telemetry["DifferentialDistance"]
            ).sum()

            # Total distance convered in the mini-sector
            total_keypoint_distance = telemetry["DifferentialDistance"].sum()
            
            # Scaled Traction Energy: Normalised by energy consumed per meter of relative distance
            scaled_traction_energy = raw_traction_energy / total_keypoint_distance
            driver_keypoint_digest[keypoint_group] = (
                scaled_traction_energy,
                total_keypoint_distance
            )

        return driver_keypoint_digest
    
    def get_lap_traction_energy(
            self, 
            driver_keypoint_map: dict[str, tuple[float, float]]
        ) -> float:
        """Calculates the Traction Energy that is used by each driver based on the telemetry 
        from the Q1 lap with a loose assumption that a Q1 lap is similar in pace to a race lap.
        The Traction Energy is defined as the energy withdrawn from the tyre for every meter 
        of a lap by mapping the interactions between Throttle, Speed and Relative Distance travelled.

        Here we rescaled the traction energies that were calculated for each mini-sector by weighing
        them on their relative distance to normalise the energy for corners and straights alike.
        
        Args:
        - driver_keypoint_map: dict[str, tuple[float, float]]
        
        Returns:
        - lap_traction_energy: float"""

        total_energy_interaction, total_distance = 0.0, 0.0

        # Iterating through all the keypoints that compose the lap
        for sector_energy, sector_distance in driver_keypoint_map.values():
            total_energy_interaction += sector_energy * sector_distance
            total_distance += sector_distance

        # Lap-wise traction energy
        lap_traction_energy = total_energy_interaction / total_distance

        return lap_traction_energy
    
    def get_keypoint_braking_energy(self, driver_telemetry: Telemetry) -> dict[str, float]:
        """Calculates the Longitudinal Braking Energy that is used by each driver based 
        on the telemetry from the Q1 lap with a loose assumption that a Q1 lap is similar 
        in pace to a race lap. The Braking Energy is defined as the energy utilsed by the car and the
        driver to brake in a straight-line / entering into a corner. It is generated by calculating the 
        deceleration based on telemetry for braking zone.
        
        Args:
        - driver_telemetry: Telemetry
        
        Returns:
        - driver_keypoint_digest: dict[str, float]"""

        # Braking Energy Registry
        braking_map = {}

        # Grouping the driver telemetry by keypoints
        grouped_tele = driver_telemetry.groupby("Keypoint", observed=False)

        # Iterating through the Braking Keypoints
        for keypoint, braking_telemetry in grouped_tele:
            if keypoint not in BRAKING_KEYS:
                continue

            # Braking Speed in m/s
            braking_speed_ms = braking_telemetry["Speed"] * (5/18)

            # Instantaneous Delta Braking Speeds
            braking_speed_diff = braking_speed_ms.diff().fillna(0.0)

            # Instantaneous Delta Braking Time in seconds
            braking_time = braking_telemetry["Date"]
            braking_time_diff = (
                braking_time
                .diff()
                .dt.total_seconds()
            )

            # Instantaneous Deceleration
            deceleration = (braking_speed_diff/braking_time_diff).fillna(0.0)
            
            # Distance-Weighted Mean Deceleration
            mean_deceleration = abs(
                (deceleration * braking_telemetry["DifferentialDistance"]).sum() /
                braking_telemetry["DifferentialDistance"].sum()
            )
            
            braking_map[keypoint] = mean_deceleration

        return braking_map
    
    def get_lap_braking_energy(
            self, 
            driver_braking_map: dict[str, float]
        ) -> float:
        """Calculates the Longitudinal Braking Energy that is used by each driver based 
        on the telemetry from the Q1 lap with a loose assumption that a Q1 lap is similar 
        in pace to a race lap. The Braking Energy is defined as the energy utilsed by the car and the
        driver to brake in a straight-line / entering into a corner.

        Here we take an average of braking energies that were calculated for each braking zone.
        
        Args:
        - driver_braking_map: dict[str, float]
        
        Returns:
        - lap_braking_energy: float"""

        lap_braking_energy = sum(driver_braking_map.values()) / len(driver_braking_map)
        return lap_braking_energy
        
    def get_keypoint_braking_force(
            self, 
            driver_braking_energy_map: dict[str, float],
            mean_fuel_burn: float
        ) -> dict[str, float]:
        """Calculates the Longitudinal Braking Force that is used by each driver based 
        on the telemetry from the Q1 lap with a loose assumption that a Q1 lap is similar 
        in pace to a race lap. The Braking Force extends the Braking energy that is calculated 
        by the pipeline based on the estimated weight of the cars and the drivers.
        
        Args:
        - driver_braking_energy_digest: dict[str, float]
        
        Returns:
        - driver_braking_force_digest: dict[str, float]"""

        # Braking Force Registry
        braking_force_map = {}

        # Iterating through the keypoints and the energy to map the force
        for keypoint, deceleration_energy in driver_braking_energy_map.items():
            braking_force_map[keypoint] = (
                (Pipeline.car_mass + Pipeline.driver_mass + mean_fuel_burn) * deceleration_energy
            )

        return braking_force_map
    
    def get_lap_braking_force(
            self, 
            driver_braking_force_map: dict[str, float]
        ) -> float:
        """Calculates the Longitudinal Braking Force that is used by each driver based 
        on the telemetry from the Q1 lap with a loose assumption that a Q1 lap is similar 
        in pace to a race lap. The Braking Force extends the Braking energy that is calculated 
        by the pipeline based on the estimated weight of the cars and the drivers.

        Here we take an average of the braking forces that were calculated for each braking zone.
        
        Args:
        - driver_braking_force_map: dict[str, float]
        
        Returns:
        - lap_braking_force: float"""

        lap_braking_force = sum(driver_braking_force_map.values()) / len(driver_braking_force_map)
        return lap_braking_force
