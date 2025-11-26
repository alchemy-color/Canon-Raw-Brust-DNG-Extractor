#!/usr/bin/env python3
"""
DNG Extractor - PyQt5 GUI

Features:
- Drag & drop .cr3 (or any file) into the window
- Select output folder, set base filename
- Batch numbering when multiple inputs
- Overall progress + per-file status
- Live log area
- Preferences persist (prefs file in user home)
- Uses subprocess to call `dnglab convert --image-index all --embed-raw false <in> <out>`
"""

import sys
import os
import json
import shutil
import subprocess
from pathlib import Path
from functools import partial
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from PyQt5 import QtCore, QtGui, QtWidgets

PREFS_PATH = Path.home() / ".dng_extractor_prefs.json"
DEFAULT_PREFS = {
    "output_folder": str(Path.home() / "Desktop"),
    "dnglab_path": "dnglab",   # allow absolute path or just name if on PATH
    "max_workers": 2
}


def load_prefs():
    if PREFS_PATH.exists():
        try:
            with open(PREFS_PATH, "r", encoding="utf-8") as f:
                prefs = json.load(f)
                return {**DEFAULT_PREFS, **prefs}
        except Exception:
            return DEFAULT_PREFS.copy()
    else:
        return DEFAULT_PREFS.copy()


def save_prefs(prefs):
    try:
        with open(PREFS_PATH, "w", encoding="utf-8") as f:
            json.dump(prefs, f, indent=2)
    except Exception as e:
        print("Failed saving prefs:", e)


class DropListWidget(QtWidgets.QListWidget):
    """
    QListWidget that accepts file drag & drop and emits signal with file list
    """
    filesDropped = QtCore.pyqtSignal(list)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setAcceptDrops(True)
        self.setSelectionMode(self.ExtendedSelection)

    def dragEnterEvent(self, e):
        if e.mimeData().hasUrls():
            e.accept()
        else:
            super().dragEnterEvent(e)

    def dropEvent(self, e):
        urls = e.mimeData().urls()
        paths = [str(u.toLocalFile()) for u in urls if u.isLocalFile()]
        if paths:
            self.filesDropped.emit(paths)


class MainWindow(QtWidgets.QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("DNG Extractor (Python)")
        self.resize(800, 600)

        self.prefs = load_prefs()

        # Layouts
        layout = QtWidgets.QVBoxLayout(self)

        header = QtWidgets.QHBoxLayout()
        header.addWidget(QtWidgets.QLabel("<b>DNG Extractor</b>"))
        header.addStretch()
        prefs_btn = QtWidgets.QPushButton("Preferences")
        prefs_btn.clicked.connect(self.open_prefs)
        header.addWidget(prefs_btn)
        layout.addLayout(header)

        # Drag & drop area + controls
        top = QtWidgets.QHBoxLayout()

        left_v = QtWidgets.QVBoxLayout()
        self.file_list = DropListWidget()
        self.file_list.filesDropped.connect(self.add_files)
        left_v.addWidget(QtWidgets.QLabel("Drop RAW burst files here (or use Add Files)"))
        left_v.addWidget(self.file_list)

        btn_row = QtWidgets.QHBoxLayout()
        add_btn = QtWidgets.QPushButton("Add Files...")
        add_btn.clicked.connect(self.select_files)
        clear_btn = QtWidgets.QPushButton("Clear")
        clear_btn.clicked.connect(self.clear_files)
        btn_row.addWidget(add_btn)
        btn_row.addWidget(clear_btn)
        left_v.addLayout(btn_row)

        top.addLayout(left_v, 2)

        right_v = QtWidgets.QFormLayout()
        self.output_folder_edit = QtWidgets.QLineEdit(self.prefs.get("output_folder", ""))
        choose_out_btn = QtWidgets.QPushButton("Choose...")
        choose_out_btn.clicked.connect(self.choose_output_folder)
        ofrow = QtWidgets.QHBoxLayout()
        ofrow.addWidget(self.output_folder_edit)
        ofrow.addWidget(choose_out_btn)
        right_v.addRow("Output Folder:", ofrow)

        self.base_name_edit = QtWidgets.QLineEdit("output")
        right_v.addRow("Output Base Name:", self.base_name_edit)

        self.start_btn = QtWidgets.QPushButton("Start")
        self.start_btn.clicked.connect(self.start_processing)
        right_v.addRow(self.start_btn)

        self.overall_progress = QtWidgets.QProgressBar()
        self.overall_progress.setValue(0)
        right_v.addRow("Overall Progress:", self.overall_progress)

        top.addLayout(right_v, 1)

        layout.addLayout(top)

        # Per-file status table
        self.status_table = QtWidgets.QTableWidget(0, 3)
        self.status_table.setHorizontalHeaderLabels(["File", "Status", "Output"])
        header = self.status_table.horizontalHeader()
        header.setSectionResizeMode(0, QtWidgets.QHeaderView.Stretch)
        header.setSectionResizeMode(1, QtWidgets.QHeaderView.ResizeToContents)
        header.setSectionResizeMode(2, QtWidgets.QHeaderView.Stretch)
        layout.addWidget(self.status_table)

        # Log
        layout.addWidget(QtWidgets.QLabel("Log:"))
        self.log_edit = QtWidgets.QPlainTextEdit()
        self.log_edit.setReadOnly(True)
        layout.addWidget(self.log_edit, 1)

        # State
        self.tasks = []  # list of (input_path, output_path, row_index)
        self.running = False
        self.executor = None

        # Initialize UI from prefs
        self.output_folder_edit.setText(self.prefs.get("output_folder", ""))
        self.dnglab_path = self.prefs.get("dnglab_path", "dnglab")
        self.max_workers = int(self.prefs.get("max_workers", 2))

    def log(self, *parts):
        t = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        s = " ".join(str(p) for p in parts)
        self.log_edit.appendPlainText(f"[{t}] {s}")

    def add_files(self, paths):
        for p in paths:
            p = str(Path(p))
            # avoid duplicates
            if any(self.status_table.item(r, 0).text() == p for r in range(self.status_table.rowCount())):
                continue
            row = self.status_table.rowCount()
            self.status_table.insertRow(row)
            item0 = QtWidgets.QTableWidgetItem(p)
            item1 = QtWidgets.QTableWidgetItem("Pending")
            item2 = QtWidgets.QTableWidgetItem("")
            self.status_table.setItem(row, 0, item0)
            self.status_table.setItem(row, 1, item1)
            self.status_table.setItem(row, 2, item2)

    def select_files(self):
        files, _ = QtWidgets.QFileDialog.getOpenFileNames(self, "Select RAW burst files", str(Path.home()), "RAW files (*.cr3 *.cr2 *.nef *.arw *.dng);;All files (*)")
        if files:
            self.add_files(files)

    def clear_files(self):
        self.status_table.setRowCount(0)

    def choose_output_folder(self):
        folder = QtWidgets.QFileDialog.getExistingDirectory(self, "Select Output Folder", self.output_folder_edit.text() or str(Path.home()))
        if folder:
            self.output_folder_edit.setText(folder)
            self.prefs["output_folder"] = folder
            save_prefs(self.prefs)

    def open_prefs(self):
        dlg = PreferencesDialog(self.prefs, self)
        if dlg.exec_() == QtWidgets.QDialog.Accepted:
            self.prefs = dlg.get_prefs()
            save_prefs(self.prefs)
            # apply prefs
            self.output_folder_edit.setText(self.prefs.get("output_folder", ""))
            self.dnglab_path = self.prefs.get("dnglab_path", "dnglab")
            self.max_workers = int(self.prefs.get("max_workers", 2))
            self.log("Preferences updated.")

    def start_processing(self):
        if self.running:
            self.log("Already running.")
            return
        rowcount = self.status_table.rowCount()
        if rowcount == 0:
            QtWidgets.QMessageBox.warning(self, "No files", "Add files to process first.")
            return

        output_folder = self.output_folder_edit.text().strip()
        if not output_folder:
            QtWidgets.QMessageBox.warning(self, "No output folder", "Please choose an output folder.")
            return
        out_folder_p = Path(output_folder)
        out_folder_p.mkdir(parents=True, exist_ok=True)

        base_name = self.base_name_edit.text().strip() or "output"

        # Build tasks with output naming
        inputs = [self.status_table.item(r, 0).text() for r in range(rowcount)]
        tasks = []
        pad = max(3, len(str(len(inputs))))
        for i, inp in enumerate(inputs, start=1):
            if len(inputs) == 1:
                outname = f"{base_name}.dng"
            else:
                outname = f"{base_name}-{str(i).zfill(pad)}.dng"
            outpath = str(out_folder_p / outname)
            tasks.append((inp, outpath))

        # store tasks and set statuses
        self.tasks = []
        for r in range(rowcount):
            self.status_table.item(r, 1).setText("Queued")
            self.status_table.item(r, 2).setText("")
            self.tasks.append((self.status_table.item(r, 0).text(), None, r))  # output path filled later

        # run in threadpool
        self.running = True
        self.start_btn.setEnabled(False)
        self.overall_progress.setValue(0)
        self.log(f"Starting processing of {len(tasks)} file(s) with {self.max_workers} worker(s).")
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
        futures = []
        for (inp, out) in tasks:
            future = self.executor.submit(self.run_dnglab, inp, out)
            futures.append(future)

        # monitor futures in background Qt timer to avoid blocking GUI
        self._monitor_futures(futures)

    def _monitor_futures(self, futures):
        # convert to list so we can modify
        futures = list(futures)
        total = len(futures)
        completed = 0

        def check():
            nonlocal completed
            still_running = False
            for f in futures:
                if f.done():
                    # handle result only once
                    if getattr(f, "_handled", False):
                        continue
                    try:
                        inp, out, success, output_text = f.result()
                    except Exception as e:
                        # should not happen, but mark as failed
                        self.log("Task exception:", e)
                        continue

                    # find row for inp
                    row = None
                    for r in range(self.status_table.rowCount()):
                        if self.status_table.item(r, 0).text() == inp:
                            row = r
                            break
                    if row is not None:
                        if success:
                            self.status_table.item(row, 1).setText("Done")
                            self.status_table.item(row, 2).setText(out)
                            self.log(f"Done: {Path(inp).name} -> {Path(out).name}")
                        else:
                            self.status_table.item(row, 1).setText("Failed")
                            self.status_table.item(row, 2).setText(out)
                            self.log(f"Failed: {Path(inp).name}. Output:\n{output_text}")
                    f._handled = True
                    completed += 1
                else:
                    still_running = True

            # update overall progress
            if total > 0:
                self.overall_progress.setValue(int((completed / total) * 100))
            else:
                self.overall_progress.setValue(100)

            if not still_running:
                # all done
                self.running = False
                self.start_btn.setEnabled(True)
                if self.executor:
                    self.executor.shutdown(wait=False)
                    self.executor = None
                self.log("All tasks finished.")
                timer.stop()

        timer = QtCore.QTimer(self)
        timer.setInterval(500)
        timer.timeout.connect(check)
        timer.start()

    def run_dnglab(self, input_path, output_path):
        """
        Runs the dnglab conversion command and returns (input_path, output_path, success, combined_output_text)
        This runs in a worker thread (not the main GUI thread)
        """
        # Update the corresponding row status to Running (via queued Qt call)
        QtCore.QMetaObject.invokeMethod(self, "set_status_running", QtCore.Qt.QueuedConnection,
                                        QtCore.Q_ARG(str, input_path), QtCore.Q_ARG(str, output_path))

        cmd = [
            self.prefs.get("dnglab_path", "dnglab"),
            "convert",
            "--image-index", "all",
            "--embed-raw", "false",
            input_path,
            output_path
        ]
        # If the dnglab path is not absolute and not on PATH it will fail.
        # We'll still capture output and propagate it back to GUI.
        try:
            # Use subprocess.run to capture output; set text mode and timeout none
            proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            output = proc.stdout or ""
            success = (proc.returncode == 0 and Path(output_path).exists())
        except FileNotFoundError as e:
            output = f"Executable not found: {e}"
            success = False
        except Exception as e:
            output = f"Error running command: {e}"
            success = False

        # Append output to log on the main thread
        QtCore.QMetaObject.invokeMethod(self, "append_log_text", QtCore.Qt.QueuedConnection,
                                        QtCore.Q_ARG(str, f"--- {Path(input_path).name} ---\n{output}\n"))
        return (input_path, output_path, success, output)

    @QtCore.pyqtSlot(str, str)
    def set_status_running(self, input_path, output_path):
        # find row and mark running
        for r in range(self.status_table.rowCount()):
            if self.status_table.item(r, 0).text() == input_path:
                self.status_table.item(r, 1).setText("Running")
                self.status_table.item(r, 2).setText(output_path)
                break

    @QtCore.pyqtSlot(str)
    def append_log_text(self, text):
        self.log_edit.appendPlainText(text)


class PreferencesDialog(QtWidgets.QDialog):
    def __init__(self, prefs, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Preferences")
        self.resize(500, 160)
        self.prefs = dict(prefs)

        layout = QtWidgets.QFormLayout(self)

        self.output_folder_edit = QtWidgets.QLineEdit(self.prefs.get("output_folder", ""))
        out_btn = QtWidgets.QPushButton("Choose...")
        out_btn.clicked.connect(self.choose_output)
        ofrow = QtWidgets.QHBoxLayout()
        ofrow.addWidget(self.output_folder_edit)
        ofrow.addWidget(out_btn)
        layout.addRow("Default Output Folder:", ofrow)

        self.dnglab_path_edit = QtWidgets.QLineEdit(self.prefs.get("dnglab_path", "dnglab"))
        layout.addRow("dnglab path (absolute or command):", self.dnglab_path_edit)

        self.max_workers_spin = QtWidgets.QSpinBox()
        self.max_workers_spin.setMinimum(1)
        self.max_workers_spin.setMaximum(8)
        self.max_workers_spin.setValue(int(self.prefs.get("max_workers", 2)))
        layout.addRow("Max concurrent jobs:", self.max_workers_spin)

        btns = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel)
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)
        layout.addRow(btns)

    def choose_output(self):
        folder = QtWidgets.QFileDialog.getExistingDirectory(self, "Select Default Output Folder", self.output_folder_edit.text() or str(Path.home()))
        if folder:
            self.output_folder_edit.setText(folder)

    def get_prefs(self):
        return {
            "output_folder": self.output_folder_edit.text().strip() or str(Path.home()),
            "dnglab_path": self.dnglab_path_edit.text().strip() or "dnglab",
            "max_workers": int(self.max_workers_spin.value())
        }


def ensure_dnglab_on_path(path_str):
    """
    Quick check whether dnglab is an executable command or absolute file
    """
    if not path_str:
        return False
    p = Path(path_str)
    if p.is_file() and os.access(str(p), os.X_OK):
        return True
    # fallback: check PATH
    found = shutil.which(path_str) is not None
    return found


def main():
    app = QtWidgets.QApplication(sys.argv)
    w = MainWindow()
    # If dnglab not present, warn user but allow them to set path in prefs
    dnglab_path = w.prefs.get("dnglab_path", "dnglab")
    if not ensure_dnglab_on_path(dnglab_path):
        QtWidgets.QMessageBox.warning(w, "dnglab not found",
                                      f"The configured dnglab executable \"{dnglab_path}\" was not found on your system PATH.\n\n"
                                      "You can continue and set the correct path in Preferences, or install dnglab and ensure it's on PATH.")
    w.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
