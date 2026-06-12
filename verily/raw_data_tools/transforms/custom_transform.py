"""Custom transform module for integrating 3rd-party algorithms.

This module provides utilities to wrap Python functions and 3rd-party algorithms
as Apache Beam transforms, allowing easy integration into data processing pipelines.
"""

from typing import Callable, Any, Iterable, Optional
import logging

import apache_beam as beam
import pandas as pd


class ApplyCustomFunction(beam.DoFn):
    """DoFn that applies a custom Python function to each element.

    This allows users to integrate their own algorithms or 3rd-party libraries
    into the data processing pipeline.

    Example:
        >>> def my_algorithm(dataframe: pd.DataFrame) -> pd.DataFrame:
        ...     # Apply custom processing
        ...     dataframe['processed'] = dataframe['value'] * 2
        ...     return dataframe
        >>>
        >>> data | beam.ParDo(ApplyCustomFunction(my_algorithm))
    """

    def __init__(self, func: Callable[[Any], Any], **kwargs):
        """Initialize the custom function transform.

        Args:
            func: Python function to apply to each element
            **kwargs: Additional keyword arguments to pass to the function
        """
        self.func = func
        self.kwargs = kwargs
        self._func_name = func.__name__ if hasattr(func, '__name__') else str(func)

    def process(self, element: Any) -> Iterable[Any]:
        """Apply the custom function to the element.

        Args:
            element: Input element (e.g., DataFrame, dict, DataPoint)

        Yields:
            Processed element(s) from the custom function
        """
        try:
            result = self.func(element, **self.kwargs)

            # Handle different return types
            if result is None:
                return  # Don't yield anything for None
            elif isinstance(result, (list, tuple)):
                # Yield each element in the sequence
                for item in result:
                    yield item
            else:
                # Yield single result
                yield result

        except Exception as e:
            logging.error(
                f"Error in custom function '{self._func_name}': {e}"
            )
            raise


class MapWithCustomFunction(beam.PTransform):
    """PTransform that maps a custom function over all elements.

    This is a convenience wrapper for ApplyCustomFunction that works as a
    PTransform, making it easier to use in pipeline construction.

    Example:
        >>> def process_imu(df: pd.DataFrame) -> pd.DataFrame:
        ...     # Calculate magnitude of acceleration
        ...     df['magnitude'] = (
        ...         df['accel_x']**2 + df['accel_y']**2 + df['accel_z']**2
        ...     ) ** 0.5
        ...     return df
        >>>
        >>> processed_data = (
        ...     raw_data
        ...     | 'Process IMU' >> MapWithCustomFunction(process_imu)
        ... )
    """

    def __init__(self, func: Callable[[Any], Any], label: Optional[str] = None, **kwargs):
        """Initialize the transform.

        Args:
            func: Python function to apply
            label: Optional label for this transform in the pipeline graph
            **kwargs: Additional keyword arguments to pass to the function
        """
        super().__init__(label=label or f'Map({func.__name__})')
        self.func = func
        self.kwargs = kwargs

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        """Apply the transform to a PCollection.

        Args:
            pcoll: Input PCollection

        Returns:
            Processed PCollection
        """
        return pcoll | beam.ParDo(ApplyCustomFunction(self.func, **self.kwargs))


class FlatMapWithCustomFunction(beam.PTransform):
    """PTransform that flat-maps a custom function over all elements.

    Similar to MapWithCustomFunction, but expects the function to return an
    iterable and flattens the results.

    Example:
        >>> def split_by_window(df: pd.DataFrame, window_size: int) -> List[pd.DataFrame]:
        ...     # Split DataFrame into fixed-size windows
        ...     windows = []
        ...     for i in range(0, len(df), window_size):
        ...         windows.append(df.iloc[i:i+window_size])
        ...     return windows
        >>>
        >>> windowed_data = (
        ...     continuous_data
        ...     | 'Split Windows' >> FlatMapWithCustomFunction(
        ...         split_by_window,
        ...         window_size=1000
        ...     )
        ... )
    """

    def __init__(self, func: Callable[[Any], Iterable[Any]], label: Optional[str] = None, **kwargs):
        """Initialize the transform.

        Args:
            func: Python function that returns an iterable
            label: Optional label for this transform
            **kwargs: Additional keyword arguments to pass to the function
        """
        super().__init__(label=label or f'FlatMap({func.__name__})')
        self.func = func
        self.kwargs = kwargs

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        """Apply the transform to a PCollection.

        Args:
            pcoll: Input PCollection

        Returns:
            Flattened PCollection
        """
        return pcoll | beam.FlatMap(lambda x: self.func(x, **self.kwargs))


class FilterWithCustomPredicate(beam.PTransform):
    """PTransform that filters elements using a custom predicate function.

    Example:
        >>> def has_sufficient_data(df: pd.DataFrame, min_rows: int = 100) -> bool:
        ...     return len(df) >= min_rows
        >>>
        >>> filtered_data = (
        ...     all_data
        ...     | 'Filter Short Sequences' >> FilterWithCustomPredicate(
        ...         has_sufficient_data,
        ...         min_rows=500
        ...     )
        ... )
    """

    def __init__(
        self,
        predicate: Callable[[Any], bool],
        label: Optional[str] = None,
        **kwargs
    ):
        """Initialize the filter transform.

        Args:
            predicate: Function that returns True to keep element, False to filter out
            label: Optional label for this transform
            **kwargs: Additional keyword arguments to pass to the predicate
        """
        super().__init__(label=label or f'Filter({predicate.__name__})')
        self.predicate = predicate
        self.kwargs = kwargs

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        """Apply the filter to a PCollection.

        Args:
            pcoll: Input PCollection

        Returns:
            Filtered PCollection
        """
        return pcoll | beam.Filter(lambda x: self.predicate(x, **self.kwargs))


# Convenience functions for common use cases

def apply_to_dataframe(
    func: Callable[[pd.DataFrame], pd.DataFrame],
    label: Optional[str] = None
) -> MapWithCustomFunction:
    """Create a transform that applies a function to each DataFrame.

    This is a convenience wrapper for MapWithCustomFunction specifically
    for DataFrame processing functions.

    Args:
        func: Function that takes a DataFrame and returns a DataFrame
        label: Optional label for the transform

    Returns:
        MapWithCustomFunction transform

    Example:
        >>> def compute_features(df: pd.DataFrame) -> pd.DataFrame:
        ...     df['mean_value'] = df['value'].mean()
        ...     df['std_value'] = df['value'].std()
        ...     return df
        >>>
        >>> features = data | apply_to_dataframe(compute_features)
    """
    return MapWithCustomFunction(func, label=label)


def apply_algorithm(
    algorithm_class: type,
    method_name: str = 'process',
    init_args: Optional[dict] = None,
    label: Optional[str] = None
) -> MapWithCustomFunction:
    """Create a transform that applies a 3rd-party algorithm class.

    This helper makes it easy to integrate algorithms that follow the
    common pattern of initialization + method call.

    Args:
        algorithm_class: Class to instantiate
        method_name: Name of the method to call (default: 'process')
        init_args: Dictionary of arguments for class initialization
        label: Optional label for the transform

    Returns:
        MapWithCustomFunction transform

    Example:
        >>> from some_library import SignalProcessor
        >>>
        >>> processed = data | apply_algorithm(
        ...     SignalProcessor,
        ...     method_name='filter',
        ...     init_args={'cutoff_freq': 50, 'order': 4}
        ... )
    """
    init_args = init_args or {}

    def wrapper(element):
        instance = algorithm_class(**init_args)
        method = getattr(instance, method_name)
        return method(element)

    wrapper.__name__ = f'{algorithm_class.__name__}.{method_name}'
    return MapWithCustomFunction(wrapper, label=label)
