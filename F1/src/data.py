from fastf1.events import Event
from fastf1.core import Session, Telemetry, Laps

from pandas import DataFrame, merge

from src.utils import (
    TELEMETRY_KEYPOINTS_BY_DIST, 
    BRAKING_KEYS,
    MULTIVARIATE_DROP_COLS
)


class DataUtils:
    """This class implements most of the operations that will be taking place
    on the raw data that was loaded from a race-weekend using the FastF1 API.
    It handles all the loading, transformations and structured storage operations."""

    # ============ Standard Methods ============
    def __init__(self, race_event: Event, cache_dir: str) -> None:
        
        self.race_event = race_event
        self.cache_dir = cache_dir

    # ============ Member Methods ============
    def load_data(self) -> tuple[Session, Session]:
        """Loads the raw data for 2 sessions corresponding to the race weekend
        that was passed during initialisation of the instance.
        
        The sessions loaded are:
        - The Qualifying Session
        - The Race Session

        Args:
        - self: Instance of the DataUtils object

        Returns:
        - (quali, race): tuple[Session, Session]"""

        # Qualifying Session
        quali = self.race_event.get_qualifying()

        # Race Session
        race = self.race_event.get_race()

        # Loading all the data corresponding to the sessions
        sessions = [quali, race]
        for session in sessions:
            session.load(laps=True, telemetry=True, weather=True, messages=True)

        return quali, race
    
    def get_throttle_map(
            self, 
            driver_quali_telemetry: Telemetry
        ) -> tuple[float, float, float]:
        """Utilises the telemetry of each driver for their fastest lap in Q1
        to identify the amount time spent on full throttle, gear changes / feathering and 
        lift and coast.
        
        Args:
        - driver_quali_telemetry: Telemetry
        
        Returns:
        - (full_throttle_percent, transition_percent, lico_percent): tuple[
            percent_full_throttle: float
            percent_feathering_throttle: float
            percent_lico_throttle: float
        ]"""

        tele_len = len(driver_quali_telemetry)

        # Throttle Params
        full_throttle_percent = len(driver_quali_telemetry[driver_quali_telemetry["Throttle"] >= 90]) / tele_len
        lico_percent = len(driver_quali_telemetry[driver_quali_telemetry["Throttle"] <= 10].count()) / tele_len
        transition_percent = 1 - full_throttle_percent - lico_percent

        return full_throttle_percent, transition_percent, lico_percent
    
    def get_fingerprint_frame(
            self,
            q1_laps: Laps,
            top_5_drivers: list[str],
            throttle_map_digest: dict[str, tuple[float, float, float]],
            keypoint_te_digest: dict[str, dict[str, tuple[float, float]]],
            keypoint_bf_digest: dict[str, dict[str, float]],
            efficiency_digest: dict[str, tuple[float, float]]
        ) -> DataFrame:
        """Utilises the engineered features of each driver from their fastest lap in Q1
        as a loose assumption for the pace and tyre degradation variables during an entire race.
        It creates the driver fingerprint which merges the Race Laps Frame with the Engineered 
        Telemetry Variables Frame.
        
        Args:
        - q1_laps: Laps,
        - top_5_driver: list[str],
        - throttle_map_digest: dict[str, tuple[float, float, float]],
        - keypoint_te_digest: dict[str, dict[str, tuple[float, float]]],
        - keypoint_bf_digest: dict[str, dict[str, float]],
        - efficiency_digest: dict[str, tuple[float, float]]
        
        Returns:
        - multivariate_df: DataFrame"""

        fingerprint_dict = {}
        
        # Iterating through the Top 5 Drivers in the race to create the fingerprint frame
        for driver_number in top_5_drivers:

            # Fastest Time
            fastest_time = (
                q1_laps
                .pick_drivers(driver_number)  # type: ignore
                .pick_fastest()["LapTime"].total_seconds()
            )
            
            # Throttle Map
            full_t, partial_t, no_t = throttle_map_digest[driver_number]

            # Cornering to Straight Line Efficiency
            driver_eff_corner, driver_eff_stl = efficiency_digest[driver_number]

            # Traction Energy by Keypoint over a Lap
            driver_te = {
                f"te_{keypoint}":te 
                for keypoint, (te, _) in keypoint_te_digest[driver_number].items()
            }
            
            # Braking Force by Keypoint for Braking Zones
            driver_bf = {
                f"bf_{keypoint}":bf
                for keypoint, bf in keypoint_bf_digest[driver_number].items()
            }

            fingerprint_dict[driver_number] = {
                **driver_te,
                **driver_bf,
                "full_throttle_percent": full_t,
                "partial_throttle_percent": partial_t,
                "no_throttle_percent": no_t,
                "cornering_efficiency": driver_eff_corner,
                "straight_line_efficiency": driver_eff_stl,
                "fastest_quali_laptime": fastest_time
            }

        # Telemetry based Fingerprint Dataframe
        fingerprint_frame = DataFrame.from_dict(
            data=fingerprint_dict,
            orient="index",
            columns=[
                *[f"te_{keypoint}" for keypoint in TELEMETRY_KEYPOINTS_BY_DIST],
                *[f"bf_{keypoint}" for keypoint in BRAKING_KEYS],
                "full_throttle_percent", "partial_throttle_percent", 
                "no_throttle_percent", "cornering_efficiency", 
                "straight_line_efficiency", "fastest_quali_laptime"
            ]
        )

        return fingerprint_frame

    def get_multivariate_frame(
            self, 
            fingerprint_frame: DataFrame,
            race_fast_laps: Laps,
            drop_cols: list[str] = MULTIVARIATE_DROP_COLS
        ) -> DataFrame:
        """Utilises the engineered features of each driver from their fastest lap in Q1 lap
        from the Fingerprint Frame and merges it with the Race Laps frame to create the final
        Multivariate Baseline Frame.
        
        Args:
        - fingerprint_frame: DataFrame
        - race_fast_laps: Laps
        - drop_cols: list[str]
        
        Returns:
        - multivariate_frame: DataFrame"""

        # Dropping the unnecessary columns from the Race Laps
        race_fast_laps = race_fast_laps.drop(drop_cols, axis=1)
        
        # Setting the DriverNumber to Index for Merging
        race_fast_laps = race_fast_laps.set_index("DriverNumber")

        # Transforming the DateTime objects into Seconds
        race_fast_laps["LapTime"] = race_fast_laps["LapTime"].dt.total_seconds()
        race_fast_laps["Sector1Time"] = race_fast_laps["Sector1Time"].dt.total_seconds()
        race_fast_laps["Sector2Time"] = race_fast_laps["Sector2Time"].dt.total_seconds()
        race_fast_laps["Sector3Time"] = race_fast_laps["Sector3Time"].dt.total_seconds()

        # Merging the Fingerprint with the Race Laps over the Index
        multivariate_df = merge(
            left=race_fast_laps, 
            right=fingerprint_frame,
            left_index=True,
            right_index=True,
        )

        # Resetting the Index back to original without DriverNumber duplication
        multivariate_df = multivariate_df.reset_index()

        return multivariate_df
