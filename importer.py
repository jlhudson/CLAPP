# importer.py

import os
import sys
from datetime import datetime, timedelta
import pandas as pd

from dataset import DataSet, Employee, Shift, WorkArea, EmploymentType, ContractStatus, Leave, LeaveType, LeaveStatus
from reportlogger import report_logger


def import_data(folder_path=".") -> DataSet:
    dataset = DataSet()
    roster_files = [f for f in os.listdir(folder_path) if "Roster Data" in f and f.endswith('.xlsx')]
    leave_files = [f for f in os.listdir(folder_path) if "Leave" in f and f.endswith('.xlsx')]

    # Check for multiple or missing files
    if len(roster_files) > 1 or len(leave_files) > 1:
        print("Error: Multiple roster or leave files detected. Ensure only one file of each type exists.")
        sys.exit(1)
    if not roster_files:
        print("Error: No roster file found.")
        sys.exit(1)
    if not leave_files:
        print("Error: No leave file found.")
        sys.exit(1)

    # Load and validate the Roster file
    roster_file_path = os.path.join(folder_path, roster_files[0])
    print(f"Processing main roster file: {roster_files[0]}")
    df_roster = pd.read_excel(roster_file_path)

    expected_headers_roster = {
        'End Time', 'Non Attended', 'Role', 'Employee', 'Comments', 'Employee Code',
        'Employment Type', 'Published', 'Location', 'Date', 'Employee Roster Name',
        'Department', 'Start Time'
    }
    found_headers_roster = set(df_roster.columns)
    if not expected_headers_roster.issubset(found_headers_roster):
        print("Error: Roster file does not contain required headers.")
        print(f"Found headers: {found_headers_roster}")
        print(f"Expected headers: {expected_headers_roster}")
        missing_headers = expected_headers_roster - found_headers_roster
        print(f"Missing headers: {missing_headers}")
        sys.exit(1)

    process_main_roster(df_roster, dataset)

    # Load and validate the Leave file
    leave_file_path = os.path.join(folder_path, leave_files[0])
    print(f"Processing leave report file: {leave_files[0]}")
    df_leave = pd.read_excel(leave_file_path)

    expected_headers_leave = {
        'Emp Code', 'Leave Type', 'Start Date', 'End Date', 'Status', 'Requested At'
    }
    found_headers_leave = set(df_leave.columns)
    if not expected_headers_leave.issubset(found_headers_leave):
        print("Error: Leave file does not contain required headers.")
        print(f"Found headers: {found_headers_leave}")
        print(f"Expected headers: {expected_headers_leave}")
        sys.exit(1)

    process_leave_report(df_leave, dataset)

    # Debugging print to check the number of employees after loading files
    print(f"Total employees loaded: {len(dataset.employees)}")

    # Sort and finalize dataset
    for employee in dataset.employees.values():
        employee.sort_shifts()
    dataset.employees = {k: v for k, v in sorted(dataset.employees.items(), key=lambda item: item[1].name)}

    return dataset


def process_main_roster(df: pd.DataFrame, dataset: DataSet):
    """Processes the main roster data to add employees and their shifts."""
    ignore_keywords = ["DNR", "UNABLE", "CANCELLED"]
    for _, row in df.iterrows():
        name = str(row['Employee']).strip()
        roster_code = str(row['Employee Roster Name']).strip()
        if not name or not roster_code or any(keyword in name.upper() for keyword in ignore_keywords):
            continue

        employee_code = row['Employee Code']
        location = str(row['Location']).strip()
        department = str(row['Department']).strip()
        role = str(row['Role']).strip()
        work_area = WorkArea(location, department, role)

        # Convert strings to enums using from_name
        employment_type_str = row['Employment Type']
        employment_type = EmploymentType.from_name(employment_type_str)

        contract_status = ContractStatus.from_roster_name(roster_code)
        date_str = str(row['Date']).split(" ")[0]
        start_time_str = row['Start Time']
        end_time_str = row['End Time']

        published = bool(row['Published'])
        comment = row['Comments']
        is_attended = not bool(row['Non Attended'])

        start_datetime = datetime.fromisoformat(f"{date_str}T{start_time_str}")
        end_datetime = datetime.fromisoformat(f"{date_str}T{end_time_str}")
        if end_datetime < start_datetime:
            end_datetime += timedelta(days=1)

        pay_cycle = Shift.calculate_pay_cycle(start_datetime)
        shift = Shift(start=start_datetime, end=end_datetime, work_area=work_area, published=published,
                      comment=comment, is_attended=is_attended, pay_cycle=pay_cycle)

        if pd.notna(employee_code):
            if employee_code not in dataset.employees:
                employee = Employee(name, employee_code, roster_code, employment_type, contract_status)
                dataset.add_employee(employee)
            dataset.employees[employee_code].add_shift(shift)
        else:
            dataset.add_unassigned_shift(shift)


def process_leave_report(df: pd.DataFrame, dataset: DataSet):
    """Processes the leave report data to update leave information for employees, with datetime handling."""
    required_headers = {'Emp Code', 'Leave Type', 'Start Date', 'End Date', 'Status', 'Requested At', 'Hours'}
    found_headers = set(df.columns)
    missing_headers = required_headers - found_headers
    if missing_headers:
        report_logger.error(f"Leave file is missing required headers: {missing_headers}")
        sys.exit(1)

    for _, row in df.iterrows():
        employee_code = str(row['Emp Code']).strip()
        leave_type_str = str(row['Leave Type']).strip()
        status = LeaveStatus.from_name(row['Status'])  # This already returns a LeaveStatus enum instance
        requested_at = pd.to_datetime(row['Requested At'])
        hours = round(min(row['Hours'], 7.6), 2)

        start_date = pd.to_datetime(row['Start Date'])
        end_date = pd.to_datetime(row['End Date'])
        leave_dates = pd.date_range(start=start_date, end=end_date).date

        leave_type = LeaveType.from_name(leave_type_str)
        if leave_type is None:
            report_logger.error(f"Unknown Leave Type: '{leave_type_str}' for Employee Code: {employee_code}")
            continue

        if employee_code in dataset.employees:
            employee = dataset.employees[employee_code]
            for leave_day in leave_dates:
                existing_leave = next((leave for leave in employee.leave_dates if leave.date == leave_day), None)

                if existing_leave:
                    total_hours = round(min(existing_leave.hours + hours, 7.6), 2)
                    existing_leave.hours = total_hours
                else:
                    leave_entry = Leave(
                        date=leave_day,
                        status=status,  # Assigning the enum instance directly
                        requested_at=requested_at,
                        hours=hours,
                        leave_type=leave_type
                    )
                    employee.add_leave(leave_entry)
        else:
            report_logger.warning(f"Employee code {employee_code} not found in dataset; leave entry skipped.")

