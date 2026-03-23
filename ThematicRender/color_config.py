import re


class ColorConfig():
    """
        Functionality for managing GDALDEM color text files,
        which are used to define color reliefs for elevation data. These files
        consist of rows specifying color mappings for elevation levels. This class adds
        support for GDALDEM color files: load, save, update elevation-color.

        Color File Format:
        - Each line in the file can be one of the following:
            1. A metadata line, beginning with "#" (comment) or "nv" (no-data value).
            2. A color mapping line, with the format:
               "<elevation> <red> <green> <blue> [<alpha>]"
               - `<elevation>`: Integer or float representing the elevation level.
               - `<red>`, `<green>`, `<blue>`: Integers (0-255) representing the RGB color.
               - `<alpha>` (optional): Integer (0-255) representing the opacity.

        Attributes:
        - misc_lines: A list to store metadata lines such as comments or 'nv' lines.
        - _data: The base color data. A list of tuples, where each tuple represents an elevation
        level and its
        associated color values.

        Methods:
        - load(path): Loads and parses the GDAL color text file, storing color mappings in
        `_data` and metadata in `misc_lines`.
        - save(): Saves the current state  to the GDAL color text file, including both color
        mappings and metadata.
        - update(): Marks the file as having unsaved changes.
        - interpolate(idx): Return an interpolated color row for insertion above row_idx.
        - delete(idx): Removes the color mapping row at the specified index in `_data`.

        Notes:
        - Position of comment lines is not preserved

        - ability to create a desaturated variant of the color text file.
        - ability to create an HSV modified variant of the color text file.  This can be
        used to create an "arid" ramp from a "humid" ramp
        """

    def __init__(self, verbose=3):
        self.misc_lines = []
        self._data = []

    def _save_data(self, f, data):
        """
        Save color data to a file, including misc lines (comments and NV lines)
        and color data lines (elevation and RGB(A) values).

        Args:
            f (file-like object): The file object to write the data to.
            data (list of tuples): Color data lines to save. Each tuple contains:
                - Elevation (float): The elevation value.
                - R, G, B (int): The red, green, and blue color components.
                - Optional A (int): The alpha (transparency) component.

        Raises:
            IOError: If an error occurs while writing to the file.
            ValueError: Unexpected format
        """
        try:
            # Write out the comments and no-value (NV) lines
            for line in self.misc_lines:
                f.write(line + '\n')

            # Write out color data lines (elevation and RGB(A) values)
            for row_num, data_line in enumerate(data):
                if len(data_line) == 5:
                    elevation, r, g, b, a = data_line
                    f.write(f"{elevation} {r} {g} {b} {a}\n")
                elif len(data_line) == 4:
                    elevation, r, g, b = data_line
                    f.write(f"{elevation} {r} {g} {b}\n")
                else:
                    raise ValueError(f"Unexpected format in row {row_num}: {data_line}")
        except IOError as e:
            raise IOError(f"Error saving data to file: {e}")

    def _load_data(self, f):
        """
        Load and parse elevation and color data from a GDAL color ramp file.

        This method reads each line in the file, skipping lines that begin with "#" or "nv",
        which are treated as miscellaneous lines. Valid data lines are parsed to extract
        elevation and color values, which are stored as tuples in a list. The list is then
        sorted in descending order by elevation.

        GDAL line format:
            1. A metadata line, beginning with "#" (comment) or "nv" (no-data value).
            2. A color mapping line, with the format:
               "<elevation> <red> <green> <blue> [<alpha>]"
               - `<elevation>`: Integer or float representing the elevation level.
               - `<red>`, `<green>`, `<blue>`: Integers (0-255) representing the RGB color.
               - `<alpha>` (optional): Integer (0-255) representing the opacity.

        Field separators can be: comma, tabulation, spaces, ':'.
        Although GDAL supports colors by using their name, instead of the RGB triplet, this will
        generate an error.

        Position of comment lines is not preserved

        Args:
            f (file object): An open file object to read lines from.

        Returns:
            list: A list of tuples containing parsed elevation and color values, sorted
                  by elevation in descending order.

        Raises:
            ValueError: If a line cannot be parsed correctly or contains invalid values.
        """
        data = []
        for line in f:
            line = line.strip()
            if line.startswith("#") or line.startswith("nv"):
                # Add comments or nodata lines to misc
                self.misc_lines.append(line)
            else:
                # Parse an Elev, RGB(A) line
                try:
                    val = self._parse_gdal_line(line)
                    if val is not None:
                        data.append(val)
                except ValueError as e:
                    raise ValueError(f"Error in line: '{line}': {str(e)}")

        # Sort data by elevation in descending order
        data.sort(key=lambda x: x[0], reverse=True)
        return data

    @staticmethod
    def parse_ramp(file_path):
        import numpy as np

        data = []
        with open(file_path, 'r') as f:
            for line_num, line in enumerate(f, 1):
                raw_line = line.strip()

                # 1. Handle Comments and Empty Lines
                # Strip trailing comments first
                if '#' in raw_line:
                    raw_line = raw_line.split('#')[0].strip()

                # If line is empty or was just a comment, skip
                if not raw_line or raw_line.startswith("nv"):
                    continue

                try:
                    # 2. Parse Data
                    val = ColorConfig._parse_gdal_line(raw_line)
                    if val:
                        data.append(val)
                except ValueError as e:
                    # 3. Report Specific Error
                    raise ValueError(
                        f"Error parsing color file '{file_path}' on line {line_num}:\n"
                        f"  Content: '{line.strip()}'\n"
                        f"  Error: {e}"
                    )

        if not data:
            raise ValueError(f"No valid color data found in {file_path}")

        #  sort and return
        data.sort(key=lambda x: x[0])
        elevations = np.array([row[0] for row in data])
        colors = np.array([row[1:] for row in data])

        return elevations, colors

    def interpolate(self, row_idx):
        """
        Return a color row interpolated between idx row and the previous row unless:
        1) If there is only one row, return a duplicate of that row.
        2) If idx is the first row, extrapolate using the first and next row.

        Args:
            row_idx (int): Interpolate/extrapolate a new row above row_idx.

        Returns:
            list: A new interpolated/extrapolated row containing elevation and color values.
        """
        current_row = list(self._data[row_idx])

        # CASE 1 - Only one row - return duplicate
        if len(self._data) == 1:
            return current_row  # Duplicate the row if it's the only one

        if row_idx == 0:
            # CASE 2 - First row - extrapolate using list of row 0 and list of row 1
            next_row = list(self._data[1])
            new_elevation = extrap(current_row[0], next_row[0])
            new_color = [extrap(current_row[i], next_row[i], 0, 255) for i in
                         range(1, len(current_row))]
        else:
            # CASE 3 - Interpolate between current row and previous row
            prev_row = list(self._data[row_idx - 1])
            new_elevation = interp(prev_row[0], current_row[0])
            new_color = [interp(prev_row[i], current_row[i]) for i in range(1, len(current_row))]

        return [new_elevation] + new_color

    def update_line(self, idx, elevation=None, colors=None):
        """
        Update a line in _data with a new elevation and/or new colors. This
        can update the colors or the elevation or both.

        Args:
            idx (int): Index of the line to update.
            elevation (int, optional): New elevation value.
            colors (list, optional): List of new RGB(A) color values.
        """
        current_line = self._data[idx]
        new_elevation = elevation if elevation is not None else current_line[0]
        new_colors = colors if colors is not None else current_line[1:]
        new_line = self.format_gdal_line(new_elevation, *new_colors)
        self.set(idx, new_line)

    @staticmethod
    def _parse_gdal_line(line):
        """
        Parse a GDAL color mapping line and return a tuple with elevation and color values.

        Args:
            line (str): A line from the GDAL color text file.

        Returns:
            tuple: A tuple containing the elevation and color values.

        Raises:
            ValueError: If the line format is invalid or if any value is not an integer or float,
                        or if color values are out of range.
        """

        # Split the line using comma, tab, or space as separators
        parts = re.split(r'[,\t\s]+', line.strip())

        # Ensure the line has the right number of components (4 or 5)
        if len(parts) < 4 or len(parts) > 5:
            raise ValueError("Invalid line format")

        # Parse elevation as a float if possible, fallback to int otherwise
        try:

            elevation = float(parts[0]) if '.' in parts[0] else int(parts[0])
            color_values = [int(value) for value in parts[1:]]
        except ValueError:
            raise ValueError(
                "Elevation must be an integer or float. Color values must be integers"
            )

        # Validate that color values are within the 0-255 range
        if not all(0 <= value <= 255 for value in color_values):
            raise ValueError("Color values must be between 0 and 255")

        # Return a tuple with elevation and color values
        return ColorConfig.format_gdal_line(elevation, *color_values)

    @staticmethod
    def format_gdal_line(elevation, r, g, b, a=None):
        """
        Format the elevation and colors into a tuple

        Args:
            elevation (int or float): Elevation value
            r (int): Red color value
            g (int): Green color value
            b (int): Blue color value
            a (int, optional): Alpha (opacity) value

        Returns:
             tuple: A tuple containing elevation and color: r, g, b, a.
        """
        return elevation, r, g, b, a


def interp(a, b):
    """ Perform integer linear interpolation between a and b."""
    return round((a + b) / 2)


def extrap(a, b, minv=None, maxv=None):
    """ Perform integer linear extrapolation using a and b. Clip to min and max."""
    if minv is None:
        return round(a - (b - a))
    else:
        return clip(round(a - (b - a)), minv, maxv)


def clip(value, min_value, max_value):
    """Clamp a value between min_value and max_value."""
    return max(min_value, min(value, max_value))
