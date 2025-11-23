
import pkgutil
import importlib
import inspect
from dagster import SensorDefinition

all_sensors = []

for finder, name, ispkg in pkgutil.walk_packages(__path__, prefix=__name__ + "."):
    try:
        module = importlib.import_module(name)
        for obj_name, obj in inspect.getmembers(module, lambda obj: isinstance(obj, SensorDefinition)):
            all_sensors.append(obj)
    except Exception as e:
        print(f"Error importing {name}: {e}")

__all__ = ["all_sensors"]
