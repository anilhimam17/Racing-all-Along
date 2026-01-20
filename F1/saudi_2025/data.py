# Fast F1 Deps
import fastf1
from fastf1.core import Session

# Data Deps
import polars as pl
import json

# Auxilary Deps
from pathlib import Path

# Codebase Deps
from src.setup import DATA_PATH, SESSION_PATH


def get_session_info(session: Session) -> None:
    """Serialises the session info as returns it."""

    # Storing the Session Info
    info = session.session_info
    info['StartDate'] = info['StartDate'].strftime("%m/%d/%Y, %H:%M:%S")
    info['EndDate'] = info['EndDate'].strftime("%m/%d/%Y, %H:%M:%S")
    info['GmtOffset'] = str(info['GmtOffset'])

    file_name = f"{info['Meeting']['Circuit']['ShortName'].lower()}_{info['Name'].lower()}.json"
    if not (SESSION_PATH / file_name).exists():
        with open(SESSION_PATH / file_name, mode="w") as json_file:
            json.dump(obj=info, fp=json_file, indent=4)


def get_session_results(session: Session) -> Path:
    """Stores the results of the session as a CSV and returns the path."""

    csv_path = DATA_PATH / f"{session.name}.csv"
    session.results.to_csv(csv_path)

    return csv_path

def get_driver_session_car_telemetry(session: Session) -> None:
    """Stores the car telemetry for the drivers."""

    # Combined Polars Dataframe
    session_driver_telemetry: pl.DataFrame | None = None
    telemetry_schema={
        "Date": pl.Datetime,
        "RPM": pl.Int32,
        "Speed": pl.Int32,
        "nGear": pl.Int8,
        "Throttle": pl.Int32,
        "Brake": pl.Boolean,
        "DRS": pl.Int8,
        "Source": pl.String,
        "Time": pl.Time,
        "SessionTime": pl.Time,
        "DriverNumber": pl.Int8
    }

    # Iterating through the drivers for the telemetry data
    car_data = session.car_data
    for driver_number in car_data.keys():
        
        # Adding the driver number col
        car_data[driver_number]["Driver Number"] = driver_number
        driver_tele_np = car_data[driver_number].to_numpy()

        if session_driver_telemetry is None:
            session_driver_telemetry = pl.DataFrame(
                data=driver_tele_np,
                schema=telemetry_schema
            )
        else:
            new_driver_telemetry = pl.DataFrame(
                data=driver_tele_np,
                schema=telemetry_schema
            )

            session_driver_telemetry = pl.concat([session_driver_telemetry, new_driver_telemetry])

    print(len(session_driver_telemetry) if session_driver_telemetry is not None else 0)


# Main Function
def main() -> None:
    """Load all the data points from the event for storage."""

    # Loading the Event by Name
    event = fastf1.get_event(
        year=2025,
        gp="jeddah"
    )

    # Loading the entire Final Practice Session
    final_pr_session = event.get_practice(number=3)
    final_pr_session.load(
        laps=True,
        telemetry=True,
        weather=True,
        messages=True
    )

    # Constructing Session Info JSON
    get_session_info(session=final_pr_session)

    # Storing the Session Results
    final_pr_res_path = get_session_results(session=final_pr_session)

    # Storing the Session Telemetry for each driver
    get_driver_session_car_telemetry(session=final_pr_session)

    # Loading the entire Qualifying Session
    quali_session = event.get_qualifying()
    quali_session.load(
        laps=True,
        telemetry=True,
        weather=True,
        messages=True
    )

    # Constructing the Session Info JSON
    get_session_info(session=quali_session)

    # Storing the Session Results
    quali_res_path = get_session_results(session=quali_session)


# Driver Code
if __name__ == "__main__":
    main()