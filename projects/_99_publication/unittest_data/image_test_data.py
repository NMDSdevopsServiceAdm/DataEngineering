import struct
import zlib

_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
_INCHES_PER_METRE = 0.0254
_RGB_8_BIT = (8, 2, 0, 0, 0)  # bit depth, colour type, compression, filter, interlace


def make_png(width: int, height: int, dpi: int = 96) -> bytes:
    """
    Builds a white PNG image of the given pixel size and resolution.

    Args:
        width (int): image width in pixels.
        height (int): image height in pixels.
        dpi (int): resolution recorded in the image.

    Returns:
        bytes: the PNG file contents.
    """

    def chunk(kind: bytes, data: bytes) -> bytes:
        body = kind + data
        return struct.pack(">I", len(data)) + body + struct.pack(">I", zlib.crc32(body))

    pixels_per_metre = round(dpi / _INCHES_PER_METRE)
    pixels = b"".join(b"\x00" + b"\xff\xff\xff" * width for _ in range(height))
    return (
        _PNG_SIGNATURE
        + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, *_RGB_8_BIT))
        + chunk(b"pHYs", struct.pack(">IIB", pixels_per_metre, pixels_per_metre, 1))
        + chunk(b"IDAT", zlib.compress(pixels))
        + chunk(b"IEND", b"")
    )
