from src.v1.config import (
    WEIGHT_TIME_CONV_CONST,
    THROTTLE_CYCLE
)


class DataPipeline:
    """This class is responsible for all the transformation operations performed
    on the FastF1 Laps DataFrames. It can generate all the new features that are
    added to the raw FastF1 Laps DataFrame."""

    # ===== Member Methods =====
    def get_effective_fuel_load(
            self, 
            max_fuel_load_in_kg: int,
            fuel_strat: float,
            fuel_sample_limit: float
        ) -> float:
        """This function estimates the effective fuel available based on 
        the provided parameters."""

        return (
            max_fuel_load_in_kg     # in kg
            - fuel_strat            # in kg
            - fuel_sample_limit     # in kg
        )
    
    def get_effective_fuel_flow(
            self,
            effective_fuel_load: float,
            fuel_flow_conv_const: float,
            hybrid_contribution: float,
            circuit_name: str,
        ) -> float:
        """This function estimates the effective fuel flow used by the team
        based on the provided parameters."""

        throttle_cycle = THROTTLE_CYCLE[circuit_name]

        return (
            effective_fuel_load         # in kg
            * fuel_flow_conv_const      # converted to gm/s
            * throttle_cycle            # fuel flow at gm/s for throttle cycle percent
            * (1 - hybrid_contribution)
        )
    
    def get_lap_fuel_burn(
            self, 
            laptime: float,
            effective_fuel_flow: float
        ) -> float:
        """Helper function to estimate the linear fuel burn in kg."""
        
        return (laptime * effective_fuel_flow) / 1000

    def get_lap_fuel_penality(
            self, 
            cumulative_fuel_burn: float,
            effective_fuel_load: float
        ) -> float:
        """Helper function to estimate the time penality to negate for Zero-Fuel pace."""

        delta_fuel_load = effective_fuel_load - cumulative_fuel_burn
        remaining_fuel_load = max(delta_fuel_load, 0.0)

        return remaining_fuel_load * WEIGHT_TIME_CONV_CONST

    def get_fuel_aware_laptime(
            self, 
            laptime: float,
            fuel_penality: float
        ) -> float:
        """Helper function to estimate the fuel-aware (Zero-Fuel pace) laptime."""
        
        return laptime - fuel_penality