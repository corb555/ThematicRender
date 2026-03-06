import sys
import numpy as np
from scipy.ndimage import gaussian_filter
from PySide6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout,
                               QHBoxLayout, QLabel, QLineEdit, QSlider, QGroupBox, QFormLayout)
from PySide6.QtCore import Qt, QTimer
from PySide6.QtGui import QImage, QPixmap

def generate_preview_noise(shape, sigmas, weights, stretch, seed):
    """The same logic from your pipeline, simplified for preview speed."""
    np.random.seed(seed)
    # Start with base white noise
    res = np.random.standard_normal(shape).astype(np.float32)

    # Apply stretch
    # (Simplified for preview: we'll just scale the gaussian sigmas per axis)

    composite = np.zeros(shape, dtype=np.float32)
    total_w = sum(weights)

    for s, w in zip(sigmas, weights):
        # Apply stretch to sigmas: sigma_y * stretch_y, sigma_x * stretch_x
        s_y = s * stretch[0]
        s_x = s * stretch[1]
        layer = gaussian_filter(res, sigma=(s_y, s_x))
        composite += (layer * (w / total_w))

    # Normalize 0..1
    c_min, c_max = composite.min(), composite.max()
    return (composite - c_min) / (c_max - c_min + 1e-6)

class NoiseEditor(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Procedural Noise Designer")
        self.setMinimumSize(900, 600)

        # State
        self.seed = 42
        self.sigmas = [1.0, 3.0, 8.0]
        self.weights = [0.7, 0.2, 0.1]
        self.stretch = [1.0, 1.0]
        self.contrast = 1.0
        self.brightness = 0.0

        self.init_ui()
        self.update_preview()

    def init_ui(self):
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        layout = QHBoxLayout(main_widget)

        # --- Sidebar (Controls) ---
        sidebar = QWidget()
        sidebar.setFixedWidth(300)
        side_layout = QVBoxLayout(sidebar)

        # 1. Profile Logic
        logic_group = QGroupBox("Noise Profile")
        form = QFormLayout(logic_group)

        self.sigma_input = QLineEdit("1.0, 3.0, 8.0")
        self.sigma_input.textChanged.connect(self.on_param_changed)
        form.addRow("Sigmas:", self.sigma_input)

        self.weight_input = QLineEdit("0.7, 0.2, 0.1")
        self.weight_input.textChanged.connect(self.on_param_changed)
        form.addRow("Weights:", self.weight_input)

        self.seed_input = QLineEdit("42")
        self.seed_input.textChanged.connect(self.on_param_changed)
        form.addRow("Seed:", self.seed_input)

        side_layout.addWidget(logic_group)

        # 2. Stretch
        stretch_group = QGroupBox("Stretch (Anisotropy)")
        stretch_form = QFormLayout(stretch_group)

        self.stretch_x = QSlider(Qt.Horizontal)
        self.stretch_x.setRange(1, 100)
        self.stretch_x.setValue(10)
        self.stretch_x.valueChanged.connect(self.on_param_changed)
        stretch_form.addRow("Stretch X:", self.stretch_x)

        self.stretch_y = QSlider(Qt.Horizontal)
        self.stretch_y.setRange(1, 100)
        self.stretch_y.setValue(10)
        self.stretch_y.valueChanged.connect(self.on_param_changed)
        stretch_form.addRow("Stretch Y:", self.stretch_y)

        side_layout.addWidget(stretch_group)

        # 3. Factor Simulation (Contrast/Brightness)
        sim_group = QGroupBox("Factor Simulation")
        sim_form = QFormLayout(sim_group)

        self.contrast_sld = QSlider(Qt.Horizontal)
        self.contrast_sld.setRange(1, 50)
        self.contrast_sld.setValue(10)
        self.contrast_sld.valueChanged.connect(self.on_param_changed)
        sim_form.addRow("Contrast:", self.contrast_sld)

        self.bright_sld = QSlider(Qt.Horizontal)
        self.bright_sld.setRange(-100, 100)
        self.bright_sld.setValue(0)
        self.bright_sld.valueChanged.connect(self.on_param_changed)
        sim_form.addRow("Bias:", self.bright_sld)

        side_layout.addWidget(sim_group)
        side_layout.addStretch()

        layout.addWidget(sidebar)

        # --- Preview Area ---
        self.preview_label = QLabel()
        self.preview_label.setAlignment(Qt.AlignCenter)
        self.preview_label.setStyleSheet("background-color: #111; border: 1px solid #333;")
        layout.addWidget(self.preview_label, 1)

    def on_param_changed(self):
        # Debounce update to avoid lag while typing
        try:
            self.sigmas = [float(x.strip()) for x in self.sigma_input.text().split(",")]
            self.weights = [float(x.strip()) for x in self.weight_input.text().split(",")]
            self.seed = int(self.seed_input.text())
            self.stretch = [self.stretch_y.value() / 10.0, self.stretch_x.value() / 10.0]
            self.contrast = self.contrast_sld.value() / 10.0
            self.brightness = self.bright_sld.value() / 100.0
            self.update_preview()
        except:
            pass # Ignore malformed input during typing

    def update_preview(self):
        # Generate 512x512 noise
        arr = generate_preview_noise((512, 512), self.sigmas, self.weights, self.stretch, self.seed)

        # Apply Factor Simulation
        # res = (res + bias) * contrast
        arr = np.clip((arr + self.brightness - 0.5) * self.contrast + 0.5, 0, 1)

        # Convert to QImage
        img_data = (arr * 255).astype(np.uint8)
        height, width = img_data.shape
        qimg = QImage(img_data.data, width, height, width, QImage.Format_Grayscale8)

        # Display
        pixmap = QPixmap.fromImage(qimg)
        self.preview_label.setPixmap(pixmap.scaled(self.preview_label.size(), Qt.KeepAspectRatio, Qt.SmoothTransformation))

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = NoiseEditor()
    window.show()
    sys.exit(app.exec())