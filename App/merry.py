import pandas as pd
import numpy as np
import sys
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QFileDialog, QMessageBox, QTableView, QHeaderView,
    QHBoxLayout,QVBoxLayout, QWidget, QProgressBar, QPushButton, QLabel, QStatusBar, QSplitter,
    QMenuBar, QMenu, QDialog, QDialogButtonBox, QTextEdit, QComboBox, QAbstractItemView,QLineEdit
)
from PySide6.QtGui import QIcon, QAction
from PySide6.QtCore import Signal, Qt, QAbstractTableModel, QObject, QRunnable, QThreadPool
import asyncio
import multiprocessing
import io

class PandasModel(QAbstractTableModel):
    def __init__(self, data):
        super(PandasModel, self).__init__()
        self._data = data

    def rowCount(self, parent=None):
        return self._data.shape[0] 

    def columnCount(self, parent=None):
        return self._data.shape[1] + 1

    def data(self, index, role=Qt.DisplayRole):
        if index.isValid():
            if role == Qt.DisplayRole:
                if index.column() == 0:  # Row index column
                    return str(index.row() + 1)
                else:
                    return str(self._data.iloc[index.row(), index.column() - 1])  # Shift column by 1 to account for row index column
        return None

    def headerData(self, col, orientation, role):
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            if col == 0:  # Row index column header
                return "#"
            else:
                return self._data.columns[col - 1]  # Shift column by 1 to account for row index column
        return None

    def setData(self, index, value, role=Qt.EditRole):
        if role == Qt.EditRole:
            row = index.row()
            col = index.column() - 1  # Adjust for row index column
            if col >= 0:
                self._data.iloc[row, col] = value
                self.dataChanged.emit(index, index, [Qt.EditRole])
                return True
        return False

    def flags(self, index):
        return Qt.ItemIsEditable | Qt.ItemIsEnabled | Qt.ItemIsSelectable

class LoadDataWorker(QRunnable):
    def __init__(self, file_path, open_with_quotes):
        super().__init__()
        self.file_path = file_path
        self.open_with_quotes = open_with_quotes
        self.data = None
        self.signals = WorkerSignals()

    def run(self):
        try:
            if self.open_with_quotes:
                self.data = pd.read_csv(self.file_path)
                arr = np.array(self.data)
                # Use np.vectorize to apply a function to each element of the array
                quote_func = np.vectorize(lambda x: '"' + str(x) + '"')
                arr = quote_func(arr)

                self.data=pd.DataFrame(arr)
            else:
                self.data = pd.read_csv(self.file_path)

            self.signals.data_loaded.emit(self.data)

        except Exception as e:
            self.signals.error.emit(str(e))

class WorkerSignals(QObject):
    data_loaded = Signal(pd.DataFrame)
    error = Signal(str)

class SortDialog(QDialog):
    def __init__(self, parent, data):
        super().__init__(parent)
        self.setWindowTitle("Sort Data")
        self.data = data

        self.column_combo = QComboBox()
        self.column_combo.addItems(data.columns)

        self.sort_order_combo = QComboBox()
        self.sort_order_combo.addItems(["Ascending", "Descending"])

        self.button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        self.button_box.accepted.connect(self.sort_data)
        self.button_box.rejected.connect(self.reject)

        layout = QVBoxLayout()
        layout.addWidget(QLabel("Column:"))
        layout.addWidget(self.column_combo)
        layout.addWidget(QLabel("Sort Order:"))
        layout.addWidget(self.sort_order_combo)
        layout.addWidget(self.button_box)
        self.setLayout(layout)

    def sort_data(self):
        column = self.column_combo.currentText()
        sort_order = self.sort_order_combo.currentText().lower() == "ascending"

        try:
            sorted_data = self.data.sort_values(by=column, ascending=sort_order)
            parent = self.parent()
            model = PandasModel(sorted_data)
            parent.table_view.setModel(model)
            self.accept()
        except Exception as e:
            QMessageBox.critical(self, "Error", str(e))

class CSVReaderApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Merry")
        self.file_path = None
        self.data = None
        self.worker_thread = None
        self.open_with_quotes = False  # Track whether to open with quotes
        self.table_view = QTableView()
        self.table_view.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.table_view.setEditTriggers(QAbstractItemView.DoubleClicked |
                                         QAbstractItemView.EditKeyPressed)

        self.progress_bar = QProgressBar()
        self.progress_bar.setMaximum(0)  # Indicates infinite progress

        preloaded_file_path = "merry.csv"
        self.load_preloaded_data(preloaded_file_path)

        self.open_button = QPushButton("Open CSV File")
        self.open_button.setCheckable(True)
        self.open_button.clicked.connect(self.open_file)

        self.save= QPushButton('SAVE')
  
        self.status_label = QLabel()

        central_widget = QWidget()
        layout = QVBoxLayout()
        splitter = QSplitter(Qt.Vertical)
        splitter.addWidget(self.table_view)
        splitter.addWidget(self.progress_bar)
        layout.addWidget(splitter)
        layout.addWidget(self.open_button)
        layout.addWidget(self.status_label)

        # Create navigation buttons
        self.up_button = QPushButton("Up")
        self.down_button = QPushButton("Down")
        self.left_button = QPushButton("Left",)
        self.right_button = QPushButton("Right")
        #css properties
        for butt in [self.up_button,self.down_button,self.left_button,self.right_button,self.save,self.open_button]:
            butt.setStyleSheet("background-color: rgb(248, 233, 191);border-radius:5px;")
            butt.setFont('Times')

        # Connect button signals to slots
        self.up_button.clicked.connect(self.navigate_up)
        self.down_button.clicked.connect(self.navigate_down)
        self.left_button.clicked.connect(self.navigate_left)
        self.right_button.clicked.connect(self.navigate_right)

        # Add navigation buttons to the layout
        button_layout = QHBoxLayout()
        button_layout.addWidget(self.up_button)
        button_layout.addWidget(self.down_button)
        button_layout.addWidget(self.left_button)
        button_layout.addWidget(self.right_button)
        layout.addLayout(button_layout)

        self.help_button = QPushButton("Help")
        self.help_button.clicked.connect(self.show_help_dialog)
        layout.addWidget(self.help_button)

        central_widget.setLayout(layout)
        self.setCentralWidget(central_widget)

        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)

        self.threadpool = QThreadPool()

        self.create_menus()

        # Set background color
        self.setStyleSheet("background-color: rgb(223, 213, 231);")

    def create_menus(self):
        menu_bar = QMenuBar()
        self.setMenuBar(menu_bar)
        
        file_menu = QMenu("&File", self)
        menu_bar.addMenu(file_menu)

        open_action = QAction("&Open CSV File", self)
        open_action.triggered.connect(self.open_file)
        file_menu.addAction(open_action)

        open_with_action = QAction("&Use Quotes", self)
        open_with_action.setCheckable(True)
        open_with_action.toggled.connect(self.toggle_open_with_quotes)
        file_menu.addAction(open_with_action)

        save_action = QAction("&Save CSV File", self)
        save_action.triggered.connect(self.save_file)
        file_menu.addAction(save_action)
    
        edit_menu = QMenu("&Edit", self)
        menu_bar.addMenu(edit_menu)

        go_to_action = QAction("&Go to...", self)
        go_to_action.triggered.connect(self.go_to_row)
        edit_menu.addAction(go_to_action)

        sort_menu = QMenu("&Sort", self)
        menu_bar.addMenu(sort_menu)

        sort_action = QAction("&Sort", self)
        sort_action.triggered.connect(self.open_sort_dialog)
        sort_menu.addAction(sort_action)

        describe_action = QAction("&Describe Data", self)
        describe_action.triggered.connect(self.describe_data)
        edit_menu.addAction(describe_action)

        column_names_action = QAction("&Get Column Names", self)
        column_names_action.triggered.connect(self.get_column_names)
        edit_menu.addAction(column_names_action)

        row_count_action = QAction("&Get Row Count", self)
        row_count_action.triggered.connect(self.get_row_count)
        edit_menu.addAction(row_count_action)

        help_menu = QMenu("&Help", self)
        menu_bar.addMenu(help_menu)

        help_action = QAction("&Help", self)
        help_action.triggered.connect(self.show_help_dialog)
        help_menu.addAction(help_action)

    def toggle_open_with_quotes(self, checked):
            self.open_with_quotes = checked

    def open_file(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Open CSV File", "", "CSV Files (*.csv)")
        if file_path:
            self.file_path = file_path
            self.status_label.setText(f"Selected file: {file_path}")
            self.load_data_async()

    def go_to_row(self):
        if self.data is not None:
            row_count = len(self.data)
            input_dialog = QDialog(self)
            input_dialog.setWindowTitle("Go to Row")
            layout = QVBoxLayout()

            row_input = QLineEdit()
            layout.addWidget(QLabel("Enter row index (0 to " + str(row_count - 1) + "):"))
            layout.addWidget(row_input)

            button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
            button_box.accepted.connect(lambda: self.handle_go_to_row(row_input.text(), input_dialog))
            button_box.rejected.connect(input_dialog.reject)
            layout.addWidget(button_box)

            input_dialog.setLayout(layout)
            input_dialog.exec()
        else:
            QMessageBox.warning(self, "Warning", "No data loaded yet.")

    def handle_go_to_row(self, row_index_text, dialog):
        try:
            row_index = int(row_index_text)
            row_count = len(self.data)
            if 0 <= row_index < row_count:
                self.table_view.selectRow(row_index)
                dialog.accept()
            else:
                QMessageBox.warning(self, "Warning", f"Row index must be between 0 and {row_count - 1}.")
        except ValueError:
            QMessageBox.warning(self, "Warning", "Invalid row index. Please enter an integer.")

    def open_sort_dialog(self):
        if self.data is not None:
            dialog = SortDialog(self, self.data)
            dialog.exec()
        else:
            QMessageBox.warning(self, "Warning", "No data loaded yet.")

    def open_file(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Open CSV File", "", "CSV Files (*.csv)")
        if file_path:
            self.file_path = file_path
            self.status_label.setText(f"Selected file: {file_path}")
            self.load_data_async()

    def load_preloaded_data(self, file_path):
        try:
            self.data = pd.read_csv(file_path)
            model = PandasModel(self.data)
            self.table_view.setModel(model)
        except Exception as e:
            QMessageBox.critical(self, "Error", str(e))

    def load_data_async(self):
        worker = LoadDataWorker(self.file_path, self.open_with_quotes)
        worker.signals.data_loaded.connect(self.data_loaded)
        worker.signals.error.connect(self.show_error)
        self.threadpool.start(worker)
        self.progress_bar.setValue(1)

    def data_loaded(self, data):
        self.data = data
        model = PandasModel(data)
        self.table_view.setModel(model)
        self.progress_bar.setMaximum(1)  # Reset progress bar
        self.status_bar.showMessage(f"Data loaded with {len(data)} rows", 10000)

    def show_error(self, error_message):
        QMessageBox.critical(self, "Error", error_message)

    def describe_data(self):
        if self.data is not None:
            dialog = QDialog(self)
            dialog.setWindowTitle("Data Description")
            layout = QVBoxLayout()
            text_edit = QTextEdit()
            text_edit.setReadOnly(True)
            text_edit.setPlainText(str(self.data.describe()))
            layout.addWidget(text_edit)
            button_box = QDialogButtonBox(QDialogButtonBox.Ok)
            button_box.accepted.connect(dialog.accept)
            layout.addWidget(button_box)
            dialog.setLayout(layout)
            dialog.exec()
        else:
            QMessageBox.warning(self, "Warning", "No data loaded yet.")

    def get_column_names(self):
        if self.data is not None:
            dialog = QDialog(self)
            dialog.setWindowTitle("Column Names")
            layout = QVBoxLayout()
            text_edit = QTextEdit()
            text_edit.setReadOnly(True)
            text_edit.setPlainText(", ".join(self.data.columns))
            layout.addWidget(text_edit)
            button_box = QDialogButtonBox(QDialogButtonBox.Ok)
            button_box.accepted.connect(dialog.accept)
            layout.addWidget(button_box)
            dialog.setLayout(layout)
            dialog.exec()
        else:
            QMessageBox.warning(self, "Warning", "No data loaded yet.")

    def get_row_count(self):
        if self.data is not None:
            dialog = QDialog(self)
            dialog.setWindowTitle("Row Count")
            layout = QVBoxLayout()
            text_edit = QTextEdit()
            text_edit.setReadOnly(True)
            text_edit.setPlainText(f"Number of rows: {len(self.data)}")
            layout.addWidget(text_edit)
            button_box = QDialogButtonBox(QDialogButtonBox.Ok)
            button_box.accepted.connect(dialog.accept)
            layout.addWidget(button_box)
            dialog.setLayout(layout)
            dialog.exec()
        else:
            QMessageBox.warning(self, "Warning", "No data loaded yet.")

    def save_file(self):
        if self.data is not None:
            file_path, _ = QFileDialog.getSaveFileName(self, "Save CSV File", "", "CSV Files (*.csv)")
            if file_path:
                self.data.to_csv(file_path, index=False)
                self.status_bar.showMessage(f"Data saved to {file_path}", 3000)
        else:
            QMessageBox.warning(self, "Warning", "No data loaded yet.")

    def navigate_up(self):
        current_index = self.table_view.currentIndex()
        new_index = current_index.sibling(current_index.row() - 1, current_index.column())
        self.table_view.setCurrentIndex(new_index)

    def navigate_down(self):
        current_index = self.table_view.currentIndex()
        new_index = current_index.sibling(current_index.row() + 1, current_index.column())
        self.table_view.setCurrentIndex(new_index)

    def navigate_left(self):
        current_index = self.table_view.currentIndex()
        new_index = current_index.sibling(current_index.row(), current_index.column() - 1)
        self.table_view.setCurrentIndex(new_index)

    def navigate_right(self):
        current_index = self.table_view.currentIndex()
        new_index = current_index.sibling(current_index.row(), current_index.column() + 1)
        self.table_view.setCurrentIndex(new_index)

    def show_help_dialog(self):
        dialog = QDialog(self)
        dialog.setWindowTitle("Help")
        layout = QVBoxLayout()

        help_text = """
        <html>
<body>
    <h1>Merry CSV Reader Help Documentation</h1>
    <p>This won't take long!</p>
    <p>Merry CSV Reader is a powerful and user-friendly application designed to open, view, and manipulate CSV (Comma-Separated Values) files. It provides a clean and intuitive interface to work with tabular data, making it an essential tool for data analysts, researchers, and anyone who needs to work with CSV files.</p>

    <h2>Advantages of Using Merry CSV Reader</h2>
    <ul>
        <li><strong>Efficient Data Handling</strong>: Merry CSV Reader utilizes the powerful pandas library, allowing you to load and process large CSV files with ease.</li>
        <li><strong>User-Friendly Interface</strong>: The application features a sleek and intuitive graphical user interface (GUI), making it easy to navigate and perform various operations on your data.</li>
        <li><strong>In-Place Editing</strong>: You can directly edit cell values within the table view by double-clicking or pressing Enter/Return, providing a convenient way to modify your data.</li>
        <li><strong>Data Analysis Tools</strong>: Merry CSV Reader offers built-in tools for data analysis, such as sorting, describing data, retrieving column names, and getting row counts.</li>
        <li><strong>Customizable Appearance</strong>: The application allows you to customize the appearance of certain UI elements, such as button styles and background colors.</li>
    </ul>

    <h2>Button Features and Usage</h2>
    <h3>File Menu</h3>
    <ul>
        <li><strong>Open CSV File</strong>: Click this button to select and open a CSV file from your file system.</li>
        <li><strong>Use Quotes</strong>: Toggle this option to open the CSV file with quotes around each field.</li>
        <li><strong>Save CSV File</strong>: Save the current data (including any modifications) to a new CSV file.</li>
    </ul>

    <h3>Navigation Buttons</h3>
    <ul>
        <li><strong>Up</strong>: Navigate to the cell directly above the current cell.</li>
        <li><strong>Down</strong>: Navigate to the cell directly below the current cell.</li>
        <li><strong>Left</strong>: Navigate to the cell to the left of the current cell.</li>
        <li><strong>Right</strong>: Navigate to the cell to the right of the current cell.</li>
    </ul>

    <h3>Edit Menu</h3>
    <ul>
        <li><strong>Go to...</strong>: Open a dialog box to enter a row index and jump directly to the specified row.</li>
        <li><strong>Sort</strong>: Open a dialog box to sort the data based on a selected column and order (ascending or descending).</li>
        <li><strong>Describe Data</strong>: Display a dialog box with a summary of descriptive statistics for the loaded data.</li>
        <li><strong>Get Column Names</strong>: Display a dialog box listing all column names in the loaded data.</li>
        <li><strong>Get Row Count</strong>: Display a dialog box showing the total number of rows in the loaded data.</li>
    </ul>

    <h3>Extra edit feature</h3>
    <ul>
    <li>You can simply edit in place a value</li>
    <li>Just DOUBLECLICK what you want to edit and write it out</li>
    <li>Navigate to file menu to save CSV</li>
    </ul>

    <h3>Help Button</h3>
    <ul>
        <li><strong>Help</strong>: Open a dialog box displaying this help documentation.</li>
    </ul>

    <h2>Feel free to donate the creator's effort</h2>
    <ul><li>briansbrian@github.com</li>
    </ul>
    <h2>Usage Instructions</h2>
    <ol>
        <li>Click the "Open CSV File" button to select a CSV file from your file system.</li>
        <li>The data from the CSV file will be loaded into the table view.</li>
        <li>Use the navigation buttons (Up, Down, Left, Right) to navigate through the cells.</li>
        <li>Double-click or press Enter/Return to edit a cell value.</li>
        <li>Use the "Sort" option from the "Sort" menu to sort the data based on a selected column.</li>
        <li>The "Edit" menu provides options to describe the data, get column names, and get the row count.</li>
        <li>To save the modified data, click the "Save CSV File" option from the "File" menu and select a file path.</li>
    </ol>
    <p>By following these instructions, you can take full advantage of Merry CSV Reader's features and efficiently work with your CSV data.</p>
</body>
</html>
        """

        text_edit = QTextEdit()
        text_edit.setReadOnly(True)
        text_edit.setHtml(help_text)
        layout.addWidget(text_edit)

        button_box = QDialogButtonBox(QDialogButtonBox.Ok)
        button_box.accepted.connect(dialog.accept)
        layout.addWidget(button_box)

        dialog.setLayout(layout)
        dialog.exec()

    def closeEvent(self, event):
        self.threadpool.clear()
        event.accept()

async def main():
    app = QApplication(sys.argv)
    app.setWindowIcon(QIcon("ic2.png"))  # Replace with your icon file path
    window = CSVReaderApp()
    window.show()
    window.show_help_dialog()
    await app.exec()

if __name__ == "__main__":
    multiprocessing.freeze_support()
    asyncio.run(main())