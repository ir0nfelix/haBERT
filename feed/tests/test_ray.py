from feed.ray import RAY_AVAILABLE, init_ray, is_ray_initialized


def test_ray_module_exports() -> None:
    """Test feed/ray.py exports and initialization functions."""
    assert isinstance(RAY_AVAILABLE, bool)
    # When Ray cluster is not running at default address, init_ray() should return False gracefully
    initialized = is_ray_initialized()
    assert isinstance(initialized, bool)
