# Fast F1 Deps
from fastf1.core import Session
from fastf1.events import Event

# Data Deps
import json

# Auxilary Deps
from pathlib import Path

# Codebase Deps
from src.setup import DATA_PATH
from src.utils import DRIVER_NUMBER_MAP


class RaceWeekend:
    def __init__(
            self, 
            event: Event, 
            practice_1: bool = False,
            practice_2: bool = True,
            practice_3: bool = True,
            qualifying: bool = True,
            race: bool = False
        ) -> None:
        """Constructor for the Custom Race Weekend class which handles loading and storing
        of all the data that corresponds to a session."""

        # The entire raceweekend as a FastF1 Event Object
        self.event = event

        # Creating the federated path for the raceweekend.
        self.race_weekend_path = (DATA_PATH / event["Country"].replace(" ", "_").lower())
        self.race_weekend_path.mkdir(exist_ok=True)

        # Loading the sessions and constructing the directories based on flags
        if practice_1:
            self.practice_1 = self.event.get_practice(number=1)
            self.practice_1.load()
            self.practice_1_path = (self.race_weekend_path / self.practice_1.name.replace(" ", "_").lower())
            self.practice_1_path.mkdir(exist_ok=True)
        if practice_2:
            self.practice_2 = self.event.get_practice(number=2)
            self.practice_2.load()
            self.practice_2_path = (self.race_weekend_path / self.practice_2.name.replace(" ", "_").lower())
            self.practice_2_path.mkdir(exist_ok=True)
        if practice_3:
            self.practice_3 = self.event.get_practice(number=3)
            self.practice_3.load()
            self.practice_3_path = (self.race_weekend_path / self.practice_3.name.replace(" ", "_").lower())
            self.practice_3_path.mkdir(exist_ok=True)
        if qualifying:
            self.qualifying = self.event.get_qualifying()
            self.qualifying.load()
            self.qualifying_path = (self.race_weekend_path / self.qualifying.name.replace(" ", "_").lower())
            self.qualifying_path.mkdir(exist_ok=True)
        if race:
            self.race = self.event.get_race()
            self.race.load()
            self.race_path = (self.race_weekend_path / self.race.name.replace(" ", "_").lower())
            self.race_path.mkdir(exist_ok=True)

    def get_session(self, session_name: str) -> tuple[Session, Path]:
        """Searches for the session based on the session name."""

        if session_name == "practice_1":
            return self.practice_1, self.practice_1_path
        elif session_name == "practice_2":
            return self.practice_2, self.practice_2_path
        elif session_name == "practice_3":
            return self.practice_3, self.practice_3_path
        elif session_name == "qualifying":
            return self.qualifying, self.qualifying_path
        elif session_name == "race":
            return self.race, self.race_path
        else:
            raise ValueError("Invalid Session Name")
    
    def get_session_info(self, session_name: str) -> dict:
        """Serialises the session info as returns it."""

        # Accessing the correct session and sub-directory path
        session, path = self.get_session(session_name=session_name)

        # Storing the Session Info
        info = session.session_info
        info['StartDate'] = info['StartDate'].strftime("%m/%d/%Y, %H:%M:%S")
        info['EndDate'] = info['EndDate'].strftime("%m/%d/%Y, %H:%M:%S")
        info['GmtOffset'] = str(info['GmtOffset'])

        file_name = "session_info.json"
        if not (path / file_name).exists():
            with open(path / file_name, mode="w") as json_file:
                json.dump(obj=info, fp=json_file, indent=4)

        return info
    
    def get_session_results(self, session_name: str) -> None:
        """Stores the results of the session as a CSV."""

        # Accessing the correct session and sub-directory path
        session, path = self.get_session(session_name=session_name)
        
        file_path = path / "results.csv"
        session.results.to_csv(file_path)
    
    def get_car_telemetry(self, session_name: str) -> None:
        """Stores the car telemetry for each driver in the session."""

        # Accessing the correct session and sub-directory path
        session, path = self.get_session(session_name=session_name)

        # Creating the sub-directory path for telemetry
        telemetry_path = path / "telemetry"
        telemetry_path.mkdir(exist_ok=True)

        # Acessing the car telemetry dict
        car_data = session.car_data

        # Storing the car telemetry data
        for driver_number in car_data.keys():
            driver_path = telemetry_path / f"{DRIVER_NUMBER_MAP[driver_number].lower()}.parquet"
            car_data[driver_number].to_parquet(index=False, path=driver_path)
            print(f"{DRIVER_NUMBER_MAP[driver_number]} data stored successfully")

    def get_session_laps(self, session_name: str) -> None:
        """Stores the laps that have taken place in the session."""

        # Accessing the correct session and sub-directory path
        session, path = self.get_session(session_name=session_name)

        file_path = path / "session_laps.parquet"
        session.laps.to_parquet(index=False, path=file_path)

    def get_weather_data(self, session_name) -> None:
        """Stores the weather data during the session."""
        
        # Accessing the correct session and sub-directory path
        session, path = self.get_session(session_name=session_name)

        file_path = path / "weather_data.parquet"
        session.weather_data.to_parquet(index=False, path=file_path)  # type: ignore
