"""Representation of arrotolata user enums in python."""

from enum import Enum


class UserIdType(Enum):
    """User ID types."""
    USER_ID_TYPE_UNKNOWN = 0
    USER_ID_TYPE_GAIA = 1
    USER_ID_TYPE_MPOWER = 2
    USER_ID_TYPE_HASHED_STRING = 3
    USER_ID_TYPE_CSP = 4
    USER_ID_TYPE_ANON_STUDY_DEVICE = 5
    USER_ID_TYPE_SENSOR_REGISTRY_CUSTOM = 6
    USER_ID_TYPE_DMI = 7
    USER_ID_TYPE_CSP_UUID = 8
    USER_ID_TYPE_FIREBASE = 9
