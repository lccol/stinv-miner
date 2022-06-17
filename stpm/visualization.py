from email.policy import default
import numpy as np
import matplotlib.pyplot as plt

from collections import defaultdict
from typing import Union, List, Dict, Optional, Tuple

def generate_points_from_pattern(pattern: List[str],
                                spatial_thr: float,
                                aperture: float,
                                center_angle: float=90) -> Tuple[np.ndarray, np.ndarray]:
    def _count_element_per_delta_space() -> Dict[int, int]:
        count_dict = defaultdict(int)
        for p in pattern:
            _, s, _ = p.split('_')
            s = int(s[1:])
            count_dict[s] += 1
        return dict(count_dict)
    theta, rho = [], []
    labels = []
    t_check = None
    min_angle = center_angle - aperture / 2
    count_dict = _count_element_per_delta_space()
    appeared = defaultdict(int)
    for p in pattern:
        event, s, t = p.split('_')
        s = int(s[1:])
        if t_check is None:
            t_check = t
        assert t_check == t

        angle_step = aperture / (count_dict[s] + 1)
        angle_offset = (appeared[s] + 1) * angle_step
        appeared[s] += 1

        rho_value = s * spatial_thr
        theta_value = min_angle + angle_offset

        theta.append(theta_value)
        rho.append(rho_value)
        labels.append(event)

    return np.radians(theta), np.array(rho), labels

def single_plot(aperture: float,
                max_radius: float,
                spatial_thr: float,
                pattern: List[str],
                ax: plt.Axes,
                center_angle: float=90) -> plt.Axes:
    if ax is None:
        _, ax = plt.subplots(subplot_kw={'projection': 'polar'})
    ax.set_thetamin(90 - aperture / 2)
    ax.set_thetamax(90 + aperture / 2)
    ax.set_ylim([0.0, max_radius])
    ax.set_xticks([])
    ax.set_yticks(np.arange(0, max_radius + 1, spatial_thr)[1:])

    theta, rho, labels = generate_points_from_pattern(pattern, spatial_thr, aperture, center_angle=center_angle)
    ax.scatter(theta, rho)
    for t, r, text in zip(theta, rho, labels):
        ax.text(t, r + 0.05 * spatial_thr, text, verticalalignment='bottom', horizontalalignment='center')

    return ax

def plot_pattern(pattern: List[List[str]],
                aperture: float,
                spatial_thr: float,
                spatial_steps: int,
                orientation: str='up'):
    if orientation in {'top', 'up'}:
        center_angle = 90
    elif orientation in {'bottom', 'down'}:
        center_angle = 270
    elif orientation in {'left'}:
        center_angle = 180
    elif orientation in {'right'}:
        center_angle = 0
    
    nplots = len(pattern)
    max_radius = spatial_steps * spatial_thr

    fig, axes = plt.subplots(1, nplots, subplot_kw={'projection': 'polar'})
    for idx, (p, ax) in enumerate(zip(pattern, axes)):
        ax.set_title(f'T = {idx}')
        single_plot(aperture, max_radius, spatial_thr, p, ax, center_angle)

    return fig
