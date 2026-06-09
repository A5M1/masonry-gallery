#ifndef VIDEO2PNG_H
#define VIDEO2PNG_H

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Initialize the library (optional, currently a no-op).
 * @return 0 on success
 */
int v2p_init(void);

/**
 * Clean up any global resources (optional, currently a no-op).
 */
void v2p_cleanup(void);

/**
 * Extract a single video frame from the given file at the specified seek time,
 * convert it to PNG, and write it to disk.
 *
 * @param video_path         Path to the input video file.
 * @param output_png_path    Path where the output PNG will be saved.
 * @param seek_time_seconds  Time in seconds from the start of the video to seek to.
 *                           Must be >= 0.
 * @param size_preset        Optional string specifying the output PNG size preset.
 * @return 0 on success, negative error code otherwise:
 *         -1 : failed to open input video file
 *         -2 : no video stream found
 *         -3 : failed to seek to requested timestamp
 *         -4 : failed to decode any video frame
 *         -5 : PNG encoding/writing failed
 *         -6 : CUDA memory allocation or kernel launch failed
 */
int v2p_extract_frame_to_png(const char *video_path, const char *output_png_path,
                             double seek_time_seconds, const char *size_preset);

#ifdef __cplusplus
}
#endif

#endif /* VIDEO2PNG_H */