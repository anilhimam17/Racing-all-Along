from fastf1.events import Event
from fastf1.core import Session

from pandas import DataFrame, cut

from src.utils import TELEMETRY_KEYPOINTS_BY_DIST


class DataUtils:
    """This class implements most of the operations that will be taking place
    on the raw data that was loaded from a race-weekend using the FastF1 API.
    It handles all the loading, transformations and structured storage operations."""

    # ============ Standard Methods ============
    def __init__(self, race_event: Event, cache_dir: str) -> None:
        
        self.race_event = race_event
        self.cache_dir = cache_dir

    # ============ Member Methods ============
    def load_data(self) -> tuple[Session, Session, Session]:
        """Loads the raw data for 3 sessions corresponding to the race weekend
        that was passed during initialisation of the instance.
        
        The sessions loaded are:
        - A Practice Session (1/2) depending on the type of the race weekend (Sprint / Normal).
        - The Qualifying Session
        - The Race Session

        Args:
        - self: Instance of the DataUtils object

        Returns:
        - tuple[practice: Session, quali: Session, race: Session]
        """
        
        # Practice - 1/2 Session for analysing Provisional Race Sims
        if "Sprint" not in self.race_event.values:
            race_sims = self.race_event.get_practice(number=2)
        else:
            race_sims = self.race_event.get_practice(number=1)

        # Qualifying Session
        quali = self.race_event.get_qualifying()

        # Race Session
        race = self.race_event.get_race()

        # Loading all the data corresponding to the sessions
        sessions = [race_sims, quali, race]
        for session in sessions:
            session.load(laps=True, telemetry=True, weather=True, messages=True)

        return race_sims, quali, race
    
    def map_telemetry_keypoints(self, copy_frame: DataFrame) -> DataFrame:
        """Performs the mapping between the Telemetry Distance channel and
        identified keypoints and returns the modified copy of the dataframe."""

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
