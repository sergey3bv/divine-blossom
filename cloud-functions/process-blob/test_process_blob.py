import importlib.util
import subprocess
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import Mock, patch


def load_process_blob_module():
    flask = types.ModuleType("flask")
    google = types.ModuleType("google")
    cloud = types.ModuleType("google.cloud")
    storage = types.ModuleType("google.cloud.storage")
    vision = types.ModuleType("google.cloud.vision")
    vision_v1 = types.ModuleType("google.cloud.vision_v1")

    class FakeFlask:
        def __init__(self, _name):
            self.name = _name

        def route(self, *_args, **_kwargs):
            def decorator(func):
                return func

            return decorator

    flask.Flask = FakeFlask
    flask.request = types.SimpleNamespace(get_json=lambda: None)
    storage.Client = object
    vision.ImageAnnotatorClient = object
    vision.Likelihood = object
    vision_v1.types = types.SimpleNamespace(Image=object, ImageSource=object)

    sys.modules["flask"] = flask
    sys.modules.setdefault("google", google)
    sys.modules["google.cloud"] = cloud
    sys.modules["google.cloud.storage"] = storage
    sys.modules["google.cloud.vision"] = vision
    sys.modules["google.cloud.vision_v1"] = vision_v1

    path = Path(__file__).with_name("main.py")
    spec = importlib.util.spec_from_file_location("process_blob_main", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ProcessBlobTests(unittest.TestCase):
    def test_flagged_content_deletes_blob_and_thumbnail(self):
        module = load_process_blob_module()
        blob = Mock()
        thumb_blob = Mock()
        bucket = Mock()
        bucket.blob.side_effect = [blob, thumb_blob]
        client = Mock()
        client.bucket.return_value = bucket

        with patch.object(module.storage, "Client", return_value=client):
            with patch.object(module, "update_metadata") as update_metadata:
                module.handle_moderation_result(
                    "bucket",
                    "hash",
                    {"is_flagged": True, "reason": "flagged", "scores": {"adult": "LIKELY"}},
                    "hash.jpg",
                )

        blob.delete.assert_called_once()
        thumb_blob.delete.assert_called_once()
        update_metadata.assert_called_once()


class DerivativeFilteringTests(unittest.TestCase):
    def setUp(self):
        self.module = load_process_blob_module()
        self.sha = "a" * 64

    def test_generated_derivatives_match_expected_shapes(self):
        self.assertTrue(self.module._is_generated_derivative(f"{self.sha}.jpg"))
        self.assertTrue(self.module._is_generated_derivative(f"{self.sha}/hls/master.m3u8"))
        self.assertTrue(self.module._is_generated_derivative(f"{self.sha}/hls/stream_720p.ts"))
        self.assertTrue(self.module._is_generated_derivative(f"{self.sha}/vtt/main.vtt"))

    def test_non_generated_paths_do_not_match(self):
        self.assertFalse(self.module._is_generated_derivative(f"{self.sha}.mp4"))
        self.assertFalse(self.module._is_generated_derivative("foo.jpg"))
        self.assertFalse(self.module._is_generated_derivative(f"{self.sha}/thumbs/file.jpg"))


class ThumbnailExtractionTests(unittest.TestCase):
    def setUp(self):
        self.module = load_process_blob_module()
        self.sha = "b" * 64
        self.blob_name = f"{self.sha}.mp4"

    def _build_storage_mocks(self):
        source_blob = Mock()
        thumb_blob = Mock()
        bucket = Mock()

        def blob_for_name(name):
            if name == self.blob_name:
                return source_blob
            if name == f"{self.sha}.jpg":
                return thumb_blob
            raise AssertionError(f"Unexpected blob name: {name}")

        bucket.blob.side_effect = blob_for_name
        client = Mock()
        client.bucket.return_value = bucket
        return client, source_blob, thumb_blob

    def test_extract_video_thumbnail_success(self):
        client, source_blob, thumb_blob = self._build_storage_mocks()

        def fake_run(cmd, capture_output, text, timeout):
            with open(cmd[-1], "wb") as out:
                out.write(b"jpeg-bytes")
            return subprocess.CompletedProcess(cmd, 0, "", "")

        with patch.object(self.module.storage, "Client", return_value=client), \
             patch.object(self.module.subprocess, "run", side_effect=fake_run) as run_mock:
            result = self.module.extract_video_thumbnail("bucket", self.blob_name)

        self.assertEqual(result, f"{self.sha}.jpg")
        source_blob.download_to_filename.assert_called_once()
        thumb_blob.upload_from_filename.assert_called_once()
        _, kwargs = thumb_blob.upload_from_filename.call_args
        self.assertEqual(kwargs["content_type"], "image/jpeg")

        cmd = run_mock.call_args.args[0]
        self.assertIn("-vf", cmd)
        vf_value = cmd[cmd.index("-vf") + 1]
        self.assertIn("select=eq(pict_type\\,I)", vf_value)

    def test_extract_video_thumbnail_returns_none_on_ffmpeg_failure(self):
        client, _, thumb_blob = self._build_storage_mocks()

        with patch.object(self.module.storage, "Client", return_value=client), \
             patch.object(
                 self.module.subprocess,
                 "run",
                 return_value=subprocess.CompletedProcess(["ffmpeg"], 1, "", "failure"),
             ):
            result = self.module.extract_video_thumbnail("bucket", self.blob_name)

        self.assertIsNone(result)
        thumb_blob.upload_from_filename.assert_not_called()

    def test_extract_video_thumbnail_returns_none_on_download_failure(self):
        client, source_blob, _ = self._build_storage_mocks()
        source_blob.download_to_filename.side_effect = Exception("download failed")

        with patch.object(self.module.storage, "Client", return_value=client), \
             patch.object(self.module.subprocess, "run") as run_mock:
            result = self.module.extract_video_thumbnail("bucket", self.blob_name)

        self.assertIsNone(result)
        run_mock.assert_not_called()

    def test_extract_video_thumbnail_returns_none_on_upload_failure(self):
        client, _, thumb_blob = self._build_storage_mocks()
        thumb_blob.upload_from_filename.side_effect = Exception("upload failed")

        def fake_run(cmd, capture_output, text, timeout):
            with open(cmd[-1], "wb") as out:
                out.write(b"jpeg-bytes")
            return subprocess.CompletedProcess(cmd, 0, "", "")

        with patch.object(self.module.storage, "Client", return_value=client), \
             patch.object(self.module.subprocess, "run", side_effect=fake_run):
            result = self.module.extract_video_thumbnail("bucket", self.blob_name)

        self.assertIsNone(result)


class ProcessBlobVideoFlowTests(unittest.TestCase):
    def setUp(self):
        self.module = load_process_blob_module()
        self.sha = "c" * 64
        self.video_blob = f"{self.sha}.mp4"
        self.thumbnail_blob = f"{self.sha}.jpg"

    def test_video_flow_uses_thumbnail_for_safety_check(self):
        safe_result = {"is_flagged": False, "scores": {"adult": "UNLIKELY"}}
        with patch.object(
            self.module, "extract_video_thumbnail", return_value=self.thumbnail_blob
        ) as extract_video_thumbnail, \
             patch.object(self.module, "check_image_safety", return_value=safe_result) as check_image_safety, \
             patch.object(self.module, "handle_moderation_result") as handle_moderation_result, \
             patch.object(self.module, "update_metadata") as update_metadata:
            self.module.process_blob_event("test-bucket", self.video_blob, "video/mp4")

        extract_video_thumbnail.assert_called_once_with("test-bucket", self.video_blob)
        check_image_safety.assert_called_once_with("test-bucket", self.thumbnail_blob)
        handle_moderation_result.assert_called_once_with(
            "test-bucket",
            self.video_blob,
            safe_result,
            self.thumbnail_blob,
            c2pa=None,
        )
        update_metadata.assert_not_called()

    def test_video_flow_sets_pending_when_thumbnail_extraction_fails(self):
        with patch.object(self.module, "extract_video_thumbnail", return_value=None), \
             patch.object(self.module, "check_image_safety") as check_image_safety, \
             patch.object(self.module, "handle_moderation_result") as handle_moderation_result, \
             patch.object(self.module, "update_metadata") as update_metadata:
            self.module.process_blob_event("test-bucket", self.video_blob, "video/mp4")

        check_image_safety.assert_not_called()
        handle_moderation_result.assert_not_called()
        update_metadata.assert_called_once_with(
            self.video_blob,
            "pending",
            None,
            None,
            c2pa=None,
        )

    def test_generated_derivatives_are_skipped(self):
        generated_paths = [
            (self.thumbnail_blob, "image/jpeg"),
            (f"{self.sha}/hls/stream_720p.ts", "video/mp2t"),
            (f"{self.sha}/vtt/transcript.vtt", "text/vtt"),
        ]

        for blob_name, content_type in generated_paths:
            with patch.object(self.module, "check_image_safety") as check_image_safety, \
                 patch.object(self.module, "extract_video_thumbnail") as extract_video_thumbnail, \
                 patch.object(self.module, "update_metadata") as update_metadata:
                self.module.process_blob_event("test-bucket", blob_name, content_type)

            check_image_safety.assert_not_called()
            extract_video_thumbnail.assert_not_called()
            update_metadata.assert_not_called()


if __name__ == "__main__":
    unittest.main()
