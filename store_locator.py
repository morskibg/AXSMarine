"""
Store locator module
"""

import time
import random


def get_nearest_store(longitude: float, latitude: float) -> dict:
    """
    :param longitude: float - current location latitude
    :param latitude: float - current location longitude
    :return: dict - nearest store id and coordinates in format {"store_id": store_id, "latitude": latitude, "longitude": longitude}
    """

    if not _valid_coordinates(latitude, longitude):
        raise ValueError("Invalid coordinates given")

    store_id = random.randint(1, 5000000)
    new_latitude, new_longitude = _get_store_coordinates(latitude, longitude)    
    time_to_sleep = round(random.uniform(0.5, 5.0), 1)
    time.sleep(time_to_sleep)

    return {"store_id": store_id, "latitude": new_latitude, "longitude": new_longitude}


def _get_store_coordinates(latitude: float, longitude: float) -> tuple:
    deviation = random.randint(-15, 15)
    new_latitude = latitude + (deviation / 100) * latitude
    new_longitude = longitude + (deviation / 100) * longitude
    if not _valid_coordinates(new_latitude, new_longitude):
        return _get_store_coordinates(latitude, longitude)
    return new_latitude, new_longitude


def _valid_coordinates(latitude: float, longitude: float) -> bool:
    return abs(latitude) <= 90 and abs(longitude) <= 180
