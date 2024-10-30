import warnings
from collections import defaultdict
from datetime import timedelta

from dataset import *
from importer import import_data
from reportlogger import report_logger
from unfilled_shift_report import UnfilledShiftReport  # Import the new class

warnings.filterwarnings("ignore", message="Cannot parse header or footer so it will be ignored")


class ReportGenerator:
    def __init__(self, dataset: DataSet):
        self.dataset = dataset

    def process_employees(self):
        """Sort and validate employees based on location, shifts, and leaves."""

        sorted_employees = sorted(
            self.dataset.employees.values(),
            key=lambda emp: (
                len(emp.work_areas),  # Number of unique locations
                sorted(work_area.location for work_area in emp.work_areas),  # Alphabetical order of locations
                emp.name  # Alphabetical order by employee name
            )
        )

        self.generate_unassigned_shifts_report()
        input(f"Unassigned Shift Report Completed. Press Enter to continue...")

        for employee in sorted_employees:
            # Sort shifts within each employee by start date and time
            employee.shifts = sorted(employee.shifts, key=lambda shift: (shift.start.date(), shift.start.time()))

            # Sort leave within each employee by date and then status
            employee.leave_dates = sorted(
                [leave for leave in employee.leave_dates if leave.status],
                key=lambda leave: (leave.date, leave.status.display_name)
            )
            # Log employee info after sorting for each
            print("\n")
            report_logger.info(f"{str(employee)}")
            self.display_leave_info(employee) # Prints the upcoming Leave for this employee.

            if not self.validate_employee(employee):
                continue

            all_checks_passed = (
                    self.generate_leave_report(employee) &
                    self.validate_paycycle_hours(employee) &
                    self.validate_fortnight_days(employee) &
                    self.validate_shift_hours_per_day(employee) &
                    self.validate_unpaid_breaks(employee) &
                    self.validate_short_shifts(employee) &
                    self.validate_shift_overlaps(employee) &
                    self.validate_on_call_restrictions(employee) &
                    self.validate_minimum_breaks_and_daily_limits(employee) &
                    self.validate_sleepover_shifts(employee) &
                    self.validate_fortnight_hours(employee) &
                    self.check_pending_or_denied_leave(employee)
            )

            if all_checks_passed:
                report_logger.info(" - All checks passed.")
            else:
                input(f"Employee {employee.name} has failed one or more checks. Press Enter to continue...")

    def validate_shift_hours_per_day(self, employee: Employee) -> bool:
        """Check if any shift or group of shifts exceed 10 hours in a 12-hour window, with detailed debugging."""
        conflicts_found = False
        shifts_by_day = defaultdict(list)

        # Organize shifts by start date for daily validation
        for shift in employee.shifts:
            shift_day = shift.start.date()
            shifts_by_day[shift_day].append(shift)

        # Validate each day's shifts for the 10-hour limit within a 12-hour span
        for day, shifts in shifts_by_day.items():
            shifts = sorted(shifts, key=lambda s: s.start)  # Sort shifts by start time
            report_logger.debug(f"--- Checking shifts for {employee.name} on {day} ---")

            for i, shift in enumerate(shifts):
                total_hours = shift.net_hours if shift.is_attended else shift.gross_hours
                window_start = shift.start
                window_end = window_start + timedelta(hours=12)

                report_logger.debug(
                    f"Starting window at {window_start.strftime('%H%M')} with initial shift {shift} "
                    f"(Hours: {total_hours})"
                )

                # Accumulate hours within a 12-hour window
                for j in range(i + 1, len(shifts)):
                    next_shift = shifts[j]
                    if next_shift.start <= window_end:
                        # Add hours based on attendance and contract break consideration
                        hours_to_add = (next_shift.net_hours if next_shift.is_attended
                                        else (next_shift.gross_hours if not employee.contract_status.is_attended_considered_break
                                              else 0))
                        total_hours += hours_to_add
                        report_logger.debug(
                            f"  Adding shift {next_shift} within 12-hour window "
                            f"(Cumulative Hours: {total_hours:.2f})"
                        )
                    else:
                        break

                # Check if accumulated hours exceed 10 hours
                if total_hours > 10:
                    conflicts_found = True
                    day_str = day.strftime("%a d/%m")
                    report_logger.warning(
                        f"{day} exceeds {total_hours:.2f} hours within 12-hour window."
                    )
                    report_logger.debug(
                        f"--- End of shift window for {day} (Hours exceeded) ---"
                    )
                else:
                    report_logger.debug(
                        f"--- End of shift window for {day} (Within limit) ---"
                    )

        return not conflicts_found

    def validate_employee(self, employee: Employee) -> bool:
        """Validate essential data fields for an employee."""
        if employee.employment_type == EmploymentType.UNKNOWN:
            print("Error: Employee Validation has UNKNOWN Employment Type.")
            return False

        if employee.contract_status == ContractStatus.UNKNOWN:
            print("Error: Employee Validation has UNKNOWN Contract IFA Status.")
            return False

        return True

    def validate_paycycle_hours(self, employee: Employee) -> bool:
        """Check if employee's worked hours + leave hours exceed the allowed hours per paycycle, considering contract status."""
        hours_by_paycycle = defaultdict(float)
        shifts_by_paycycle = defaultdict(list)
        leaves_by_paycycle = defaultdict(list)

        # Collect net or gross hours from shifts based on contract status
        for shift in employee.shifts:
            if employee.contract_status.is_attended_considered_break:
                hours_to_add = shift.net_hours if shift.is_attended else shift.gross_hours
            else:
                hours_to_add = shift.gross_hours

            if shift.is_attended:
                hours_by_paycycle[shift.pay_cycle] += hours_to_add
                shifts_by_paycycle[shift.pay_cycle].append(shift)

        # Collect hours from leave, but skip if employee is CASUAL
        if employee.employment_type != EmploymentType.CASUAL:
            for leave in employee.leave_dates:
                if leave.leave_type.counts_towards_hours and leave.status.is_approved:
                    pay_cycle = Shift.calculate_pay_cycle(leave.date)
                    hours_by_paycycle[pay_cycle] += leave.calculate_hours()
                    leaves_by_paycycle[pay_cycle].append(leave)

        # Check and log exceeded hours with debugging information
        conflicts_found = False
        max_hours = employee.employment_type.hours_per_paycycle

        for pay_cycle, total_hours in hours_by_paycycle.items():
            if total_hours > max_hours:
                conflicts_found = True
                report_logger.warning(
                    f"Paycycle {self.get_paycycle_dates(pay_cycle)} exceeds max hours with "
                    f"{total_hours:.2f} hours (Allowed: {max_hours})."
                )
                report_logger.debug(f"--- Debug Info for {employee.name} in Paycycle {pay_cycle} ---")
                for shift in shifts_by_paycycle[pay_cycle]:
                    report_logger.debug(f"SHIFT: {shift}")
                for leave in leaves_by_paycycle[pay_cycle]:
                    report_logger.debug(f"LEAVE: {leave}")
                report_logger.debug(f"--- End Debug Info ---")

        return not conflicts_found

    def validate_fortnight_days(self, employee: Employee) -> bool:
        """Determine if employee has exceeded the maximum allowed days of work per fortnight."""
        days_worked_by_paycycle = defaultdict(set)  # Track unique work days by paycycle

        # Process shifts to count days worked
        for shift in employee.shifts:
            # Determine applicable days based on shift start and end
            start_date = shift.start.date()
            end_date = shift.end.date() if shift.end.time() != timedelta(0) else shift.end.date() - timedelta(days=1)

            # Check if shift counts towards days worked
            counts_as_day_worked = (
                shift.is_attended if employee.contract_status.is_attended_considered_break
                else True  # counts as work day regardless of `is_attended`
            )

            # Record days worked if applicable
            if counts_as_day_worked:
                pay_cycle = shift.pay_cycle
                days_worked_by_paycycle[pay_cycle].add(start_date)
                days_worked_by_paycycle[pay_cycle].add(end_date)

        # Process leave days for non-casual employees
        if employee.employment_type != EmploymentType.CASUAL:
            for leave in employee.leave_dates:
                if leave.leave_type.counts_towards_hours and leave.status.is_approved:
                    leave_day = leave.date
                    pay_cycle = Shift.calculate_pay_cycle(leave_day)
                    days_worked_by_paycycle[pay_cycle].add(leave_day)

        # Check if any pay cycle exceeds the maximum days allowed
        conflicts_found = False
        max_days = employee.employment_type.days

        for pay_cycle, days_worked in days_worked_by_paycycle.items():
            if len(days_worked) > max_days:
                conflicts_found = True
                report_logger.warning(
                    f"Paycycle {self.get_paycycle_dates(pay_cycle)} exceeds max days with "
                    f"{len(days_worked)} days worked (Allowed: {max_days})."
                )
                report_logger.debug(f"--- Detailed Days Worked for {employee.name} in Paycycle {pay_cycle} ---")
                for day in sorted(days_worked):
                    report_logger.debug(f"Worked Day: {day}")
                report_logger.debug(f"--- End of Detailed Days ---")

        return not conflicts_found

    def generate_leave_report(self, employee: Employee) -> bool:
        conflicts_found = False

        approved_leaves = sorted(
            [leave for leave in employee.leave_dates if leave.status.is_approved],
            key=lambda leave: leave.date
        )

        for leave in approved_leaves:
            leave_date = leave.date
            report_logger.debug(f"LEAVE ENTRY: {leave}")

            # Check for conflicts between leave and shifts on the same day
            conflicting_shifts = [
                shift for shift in employee.shifts
                if shift.start.date() <= leave_date <= shift.end.date()
            ]

            if conflicting_shifts:
                conflicts_found = True
                report_logger.error(f"Leave Conflict for {leave}:")
                for shift in conflicting_shifts:
                    report_logger.error(f"\t- {shift}")

            # Find and log the previous and next shifts around the leave date
            previous_shift, next_shift = self.find_adjacent_shifts(employee, leave_date)
            if previous_shift:
                report_logger.debug(f"PREV: {previous_shift}")
            if next_shift:
                report_logger.debug(f"NEXT: {next_shift}")

        return not conflicts_found

    def find_adjacent_shifts(self, employee: Employee, leave_date: datetime.date):
        sorted_shifts = sorted(employee.shifts, key=lambda s: s.start)
        previous_shift, next_shift = None, None

        for shift in sorted_shifts:
            if shift.end.date() < leave_date:
                previous_shift = shift
            elif shift.start.date() > leave_date:
                next_shift = shift
                break

        return previous_shift, next_shift

    def validate_unpaid_breaks(self, employee: Employee) -> bool:
        """Check for missing unpaid breaks and standalone unpaid breaks at start or end of the day."""
        conflicts_found = False
        employee_shifts = sorted(employee.shifts, key=lambda shift: shift.start)

        for i in range(len(employee_shifts) - 1):
            current_shift = employee_shifts[i]
            next_shift = employee_shifts[i + 1]

            # Calculate the gap between consecutive shifts
            gap = (next_shift.start - current_shift.end).total_seconds() / 60

            # Check if gap is between 15 minutes and 1 hour
            if 15 <= gap <= 60:
                # Sum hours of shifts around the gap
                hours_before = current_shift.net_hours if current_shift.is_attended else current_shift.gross_hours
                hours_after = next_shift.net_hours if next_shift.is_attended else next_shift.gross_hours

                # Adjust for attended shifts based on contract status
                if not employee.contract_status.is_attended_considered_break:
                    hours_before = current_shift.gross_hours
                    hours_after = next_shift.gross_hours

                total_hours = hours_before + hours_after

                # If total hours are less than 5, we need an unpaid break
                if total_hours < 5:
                    conflicts_found = True
                    break_start = current_shift.end
                    break_end = next_shift.start
                    report_logger.warning(
                        f"Missing unpaid break: {break_start.strftime('%a %d/%m %H%M')} - {break_end.strftime('%H%M')}. Total hours: {total_hours:.2f}"
                    )
                    # Debugging details
                    report_logger.debug(f"\tBefore Gap: {current_shift}")
                    report_logger.debug(f"\tAfter Gap: {next_shift}")

            # Additional check for standalone unpaid breaks
            if not current_shift.is_attended and 15 <= current_shift.gross_hours * 60 <= 60:
                # Check if it's the first or last shift of the day without shifts before or after
                day_shifts = [shift for shift in employee_shifts if shift.start.date() == current_shift.start.date()]
                is_standalone = (current_shift == day_shifts[0] and len(day_shifts) == 1) or \
                                (current_shift == day_shifts[-1] and len(day_shifts) == 1)

                if is_standalone:
                    conflicts_found = True
                    report_logger.warning(
                        f"Standalone unpaid break: {current_shift}"
                    )
                    report_logger.debug(f" Unpaid Break Shift: {current_shift}")

        return not conflicts_found

    def validate_short_shifts(self, employee: Employee) -> bool:
        """Identify and report shifts that are less than 2 hours, unless consecutive shifts bring total to 2+ hours."""
        conflicts_found = False
        employee_shifts = sorted(employee.shifts, key=lambda shift: shift.start)
        consecutive_required = 2  # Minimum consecutive hours required

        for i, shift in enumerate(employee_shifts):
            # Skip shifts that are not attended if the contract allows ignoring them
            if not shift.is_attended and employee.contract_status.is_attended_considered_break:
                continue

            # Start with any short shift (under 2 hours)
            if shift.gross_hours < consecutive_required:
                total_hours = shift.gross_hours
                consecutive_shifts = [shift]

                # Accumulate exactly consecutive shifts before the current short shift
                for j in range(i - 1, -1, -1):
                    prev_shift = employee_shifts[j]

                    # Stop if shifts are not precisely consecutive
                    if prev_shift.end != consecutive_shifts[0].start:
                        break

                    # Include previous shift based on contract rules
                    if prev_shift.is_attended or not employee.contract_status.is_attended_considered_break:
                        total_hours += prev_shift.gross_hours
                        consecutive_shifts.insert(0, prev_shift)
                        if total_hours >= consecutive_required:
                            break
                    else:
                        break

                # Accumulate exactly consecutive shifts after the current short shift
                for j in range(i + 1, len(employee_shifts)):
                    next_shift = employee_shifts[j]

                    # Stop if shifts are not precisely consecutive
                    if consecutive_shifts[-1].end != next_shift.start:
                        break

                    # Include next shift based on contract rules
                    if next_shift.is_attended or not employee.contract_status.is_attended_considered_break:
                        total_hours += next_shift.gross_hours
                        consecutive_shifts.append(next_shift)
                        if total_hours >= consecutive_required:
                            break
                    else:
                        break

                # Log a warning if accumulated hours don’t meet the requirement
                if total_hours < consecutive_required:
                    conflicts_found = True
                    report_logger.warning(
                        f"Short shift detected: {shift} ({shift.gross_hours:.2f} hrs)"
                    )
                    report_logger.debug("Shift does not meet the 2-hour requirement with consecutive shifts.")
                    for consecutive_shift in consecutive_shifts:
                        report_logger.debug(f" Consecutive Shift: {consecutive_shift}")

        return not conflicts_found

    def validate_shift_overlaps(self, employee: Employee) -> bool:
        """Detect overlapping shifts, grouping them by continuous overlap and printing the group as a list."""
        conflicts_found = False
        employee_shifts = sorted(employee.shifts, key=lambda shift: shift.start)
        reported_shifts = set()  # To track shifts already included in an overlap group

        def find_overlapping_group(start_index):
            """Finds all shifts overlapping with the shift at start_index and returns them as a group."""
            overlapping_group = [employee_shifts[start_index]]
            for j in range(start_index + 1, len(employee_shifts)):
                next_shift = employee_shifts[j]

                # Stop if no overlap is possible (next shift starts after the end of the current last shift)
                if next_shift.start >= overlapping_group[-1].end:
                    break

                # Add to group if overlapping with the last shift in the current group
                if overlapping_group[-1].start < next_shift.end:
                    overlapping_group.append(next_shift)

            return overlapping_group

        # Process each shift, detecting groups of overlapping shifts
        for i in range(len(employee_shifts)):
            current_shift = employee_shifts[i]

            # Skip shifts under 15 minutes or already reported in an overlap group
            if current_shift.gross_hours * 60 < 15 or current_shift in reported_shifts:
                continue

            # Find all overlapping shifts for the current shift
            overlapping_group = find_overlapping_group(i)

            # Only report if we have more than one shift in the group (indicating overlap)
            if len(overlapping_group) > 1:
                conflicts_found = True
                report_logger.error(f"Overlap group detected on {current_shift.start.strftime('%a %d/%m')}:")
                for shift in overlapping_group:
                    report_logger.error(f" - With: {shift}")
                    reported_shifts.add(shift)  # Mark as reported to avoid redundant processing

        return not conflicts_found

    def validate_on_call_restrictions(self, employee: Employee) -> bool:
        """Ensure that an on-call employee (shift < 15 minutes) is not scheduled for a shift the next day."""
        conflicts_found = False
        employee_shifts = sorted(employee.shifts, key=lambda shift: shift.start)

        for i in range(len(employee_shifts)):
            current_shift = employee_shifts[i]

            # Check if the shift is an on-call shift (less than 15 minutes)
            if current_shift.gross_hours * 60 >= 15:
                continue

            # Define the next day after the on-call shift
            next_day = current_shift.start.date() + timedelta(days=1)

            # Find any shifts scheduled on the next day
            next_day_shifts = [
                shift for shift in employee_shifts
                if shift.start.date() == next_day
            ]

            # If there are shifts on the next day, log a conflict
            if next_day_shifts:
                conflicts_found = True
                report_logger.warning(f"On-call restriction violated: {current_shift} is followed by work on {next_day}.")
                report_logger.debug(f" On-call Shift: {current_shift}")
                for offending_shift in next_day_shifts:
                    report_logger.debug(f" Offending Next Day Shift: {offending_shift}")

        return not conflicts_found

    def validate_minimum_breaks_and_daily_limits(self, employee: Employee) -> bool:
        """Check that employees only have one 12-hour work window per day, end all shifts within 12 hours, and meet break requirements."""
        conflicts_found = False
        employee_shifts = sorted(employee.shifts, key=lambda shift: shift.start)
        minimum_break_hours = employee.contract_status.minimum_break
        is_attended_break = employee.contract_status.is_attended_considered_break

        i = 0
        last_window_end = None
        last_shift_end = None
        current_day = None

        while i < len(employee_shifts):
            shift = employee_shifts[i]

            # Skip shifts less than 15 minutes (as per on-call rule)
            if shift.gross_hours * 60 < 15:
                i += 1
                continue

            # Start a new 12-hour window if no current window or a new day
            if last_window_end is None or shift.start.date() != current_day:
                window_start = shift.start
                window_end = window_start + timedelta(hours=12)
                current_day = shift.start.date()
                total_hours_in_window = 0
                last_shift_end = shift.end  # Track end of the last contributing shift in this 12-hour window

                # Accumulate hours within the 12-hour window and ensure valid shifts end within this window
                for j in range(i, len(employee_shifts)):
                    next_shift = employee_shifts[j]

                    # Stop if the shift starts on a new day or if it starts after the window ends
                    if next_shift.start.date() != current_day or next_shift.start >= window_end:
                        break

                    # If is_attended_break is True, only attended shifts count as work; otherwise, all shifts count
                    if is_attended_break:
                        if next_shift.is_attended:
                            total_hours_in_window += next_shift.gross_hours
                            last_shift_end = max(last_shift_end, next_shift.end)  # Update last shift end only if it’s attended
                    else:
                        # Count all hours as work if contract does not consider attended as break
                        total_hours_in_window += next_shift.gross_hours
                        last_shift_end = max(last_shift_end, next_shift.end)  # Update last shift end for all shifts

                    # Ensure each shift that counts towards work ends within the 12-hour window
                    if next_shift.is_attended or not is_attended_break:
                        if next_shift.end > window_end:
                            conflicts_found = True
                            report_logger.warning(f"Shift exceeds 12-hour window: Shift from {next_shift} ends after window ending at {window_end.strftime('%H%M')}.")
                            report_logger.debug(f" Violating Shift: {next_shift}")

                # Check if total hours in the window exceed 12
                if total_hours_in_window > 12:
                    conflicts_found = True
                    report_logger.warning(f"Overtime: {total_hours_in_window:.2f} hours worked in 12-hour window starting {window_start.strftime('%a %d/%m %H%M')}.")

                # Set `last_window_end` as the end of the 12-hour window and update `i`
                last_window_end = window_end
                i = j - 1  # Move `i` to the last shift within the window

            # Check if the employee starts a new 12-hour block on the next day with enough break
            if i + 1 < len(employee_shifts):
                next_shift = employee_shifts[i + 1]
                if next_shift.start.date() != current_day:
                    # Calculate the time gap between the last shift end and the next shift
                    time_gap = (next_shift.start - last_shift_end).total_seconds() / 3600

                    # If the time gap is less than the required minimum break, log a conflict
                    if time_gap < minimum_break_hours:
                        conflicts_found = True
                        report_logger.warning(
                            f"Insufficient break between 12-hour windows: "
                            f"only {time_gap:.2f} hours between last shift ending {last_shift_end.strftime('%a %d/%m %H%M')} "
                            f"and new 12-hour block starting {next_shift.start.strftime('%a %d/%m %H%M')}."
                        )
                        report_logger.debug(f" End of Previous 12-hour Window Shift: {employee_shifts[i]}")
                        report_logger.debug(f" New Day Start Shift: {next_shift}")

            i += 1

        return not conflicts_found

    def validate_sleepover_shifts(self, employee: Employee) -> bool:
        """Check compliance with sleepover shift requirements and surrounding work/break restrictions."""
        conflicts_found = False
        employee_shifts = sorted(employee.shifts, key=lambda shift: shift.start)
        minimum_break_hours = employee.contract_status.minimum_break
        max_work_hours = 10  # Maximum allowable work around sleepover if allowed
        sleepover_window_hours = 12

        for i, shift in enumerate(employee_shifts):
            # Identify the sleepover shift (8 hours, isAttended=False, spanning midnight)
            if shift.gross_hours <= 8 and not shift.is_attended and shift.start.hour < 23 and shift.end.hour > 0:
                sleepover_shift = shift

                # Check for the 4-hour component before or after the sleepover shift
                preceding_shift = employee_shifts[i - 1] if i > 0 else None
                following_shift = employee_shifts[i + 1] if i + 1 < len(employee_shifts) else None
                four_hour_shift = None

                if preceding_shift and preceding_shift.gross_hours == 4 and preceding_shift.is_attended:
                    four_hour_shift = preceding_shift
                elif following_shift and following_shift.gross_hours == 4 and following_shift.is_attended:
                    four_hour_shift = following_shift

                # Validate the presence and timing of the 4-hour shift
                if not four_hour_shift:
                    conflicts_found = True
                    report_logger.warning(f"Missing or incorrect 4-hour sleepover component: {sleepover_shift}.")
                    report_logger.debug(f" Sleepover Shift: {sleepover_shift}")
                    continue

                # Check work restrictions around sleepover based on contract status
                if employee.contract_status.is_allowed_work_around_sleepover:
                    # Calculate total hours within 12 hours before and after the sleepover shift
                    hours_before = sum(
                        s.gross_hours for s in employee_shifts
                        if s.end <= sleepover_shift.start and (sleepover_shift.start - s.end).total_seconds() / 3600 <= sleepover_window_hours
                    )
                    hours_after = sum(
                        s.gross_hours for s in employee_shifts
                        if s.start >= sleepover_shift.end and (s.start - sleepover_shift.end).total_seconds() / 3600 <= sleepover_window_hours
                    )

                    # Report if hours exceed the maximum allowed
                    if hours_before > max_work_hours or hours_after > max_work_hours:
                        conflicts_found = True
                        report_logger.warning(f"Excessive work hours around sleepover: {hours_before:.2f} hours before or {hours_after:.2f} hours after.")
                        report_logger.debug(f" Sleepover Shift: {sleepover_shift}")
                        report_logger.debug(f" 4-Hour Component: {four_hour_shift}")

                else:
                    # If no work is allowed around sleepover, ensure no shifts except the 4-hour one
                    for s in employee_shifts:
                        if ((s.end <= sleepover_shift.start and (sleepover_shift.start - s.end).total_seconds() / 3600 <= sleepover_window_hours) or
                            (s.start >= sleepover_shift.end and (s.start - sleepover_shift.end).total_seconds() / 3600 <= sleepover_window_hours)) and s != four_hour_shift:
                            conflicts_found = True
                            report_logger.warning(f"Unauthorized work around sleepover on {sleepover_shift}.")
                            report_logger.debug(f" Sleepover Shift: {sleepover_shift}")
                            report_logger.debug(f" Unauthorized Shift: {s}")
                            break

                # Verify minimum break compliance if no work around sleepover is allowed
                if not employee.contract_status.is_allowed_work_around_sleepover:
                    if i > 0:
                        time_before = (sleepover_shift.start - employee_shifts[i - 1].end).total_seconds() / 3600
                        if time_before < minimum_break_hours:
                            conflicts_found = True
                            report_logger.warning(f"Insufficient break before sleepover: only {time_before:.2f} hours before {sleepover_shift}.")
                            report_logger.debug(f" Sleepover Shift: {sleepover_shift}")
                            report_logger.debug(f" Previous Shift: {employee_shifts[i - 1]}")

                    if i + 1 < len(employee_shifts):
                        time_after = (employee_shifts[i + 1].start - sleepover_shift.end).total_seconds() / 3600
                        if time_after < minimum_break_hours:
                            conflicts_found = True
                            report_logger.warning(f"Insufficient break after sleepover for {employee.name}: only {time_after:.2f} hours after {sleepover_shift}.")
                            report_logger.debug(f" Sleepover Shift: {sleepover_shift}")
                            report_logger.debug(f" Following Shift: {employee_shifts[i + 1]}")

        return not conflicts_found

    def validate_fortnight_hours(self, employee: Employee) -> bool:
        """Check if the employee has less than 24 hours worked in a fortnight, including applicable leave."""
        hours_by_paycycle = defaultdict(float)

        # Aggregate hours from shifts
        for shift in employee.shifts:
            hours_by_paycycle[shift.pay_cycle] += shift.net_hours if shift.is_attended else shift.gross_hours

        # Include hours from approved leave types that count toward hours worked
        for leave in employee.leave_dates:
            if leave.status.is_approved and leave.leave_type.counts_towards_hours:
                pay_cycle = Shift.calculate_pay_cycle(leave.date)
                hours_by_paycycle[pay_cycle] += leave.calculate_hours()

        # Check each pay cycle for minimum hour requirement
        conflicts_found = False
        for pay_cycle, total_hours in hours_by_paycycle.items():
            if total_hours < 12:  # It's actually 24, but Employees fall below this all the time, we only need the worst case scenarios.
                conflicts_found = True
                report_logger.warning(
                    f"{employee.name} has less than 24 hours in Paycycle {self.get_paycycle_dates(pay_cycle)} "
                    f"(Total: {total_hours:.2f} hours)"
                )

        return not conflicts_found

    @staticmethod
    def get_paycycle_dates(pay_cycle_num: int) -> str:
        """Return the start and end dates of a given pay cycle number in the specified format."""
        # Base date from which pay cycles are calculated
        start_fortnight = datetime(2024, 7, 2)

        # Calculate start date of the pay cycle
        start_date = start_fortnight + timedelta(days=(pay_cycle_num - 1) * 14)
        end_date = start_date + timedelta(days=13)

        # Format the result
        return f"{pay_cycle_num} from {start_date.strftime('%a %d/%m')} - {end_date.strftime('%a %d/%m')}"

    def generate_unassigned_shifts_report(self):
        # Create and run the unfilled shift report
        unfilled_shift_report = UnfilledShiftReport(dataset)
        unfilled_shift_report.generate_report()

    def display_leave_info(self, employee: Employee):
        """Display approved leave ranges for the employee."""
        approved_leaves = sorted(
            [leave for leave in employee.leave_dates if leave.status == LeaveStatus.APPROVED],
            key=lambda leave: leave.date
        )

        leave_ranges = []
        current_range = None

        for leave in approved_leaves:
            if current_range and leave.date == current_range[-1] + timedelta(days=1):
                current_range.append(leave.date)
            else:
                if current_range:
                    leave_ranges.append(f"{current_range[0].strftime('%d/%m')} - {current_range[-1].strftime('%d/%m')}")
                current_range = [leave.date]

        if current_range:
            leave_ranges.append(f"{current_range[0].strftime('%d/%m')} - {current_range[-1].strftime('%d/%m')}")

        if leave_ranges:
            report_logger.info(f"Employee: {employee.name}, Approved Leave Ranges: {', '.join(leave_ranges)}")

    def check_pending_or_denied_leave(self, employee: Employee) -> bool:
        """Check for any pending or denied leave within the next 45 days and return False if found."""
        current_date = datetime.now().date()
        check_period = current_date + timedelta(days=45)

        action_required = False
        for leave in employee.leave_dates:
            if leave.status in {LeaveStatus.REQUESTED, LeaveStatus.DENIED} and current_date <= leave.date <= check_period:
                action_required = True
                report_logger.warning(f"Action Required - Employee: {employee.name}, Leave: {leave}")

        return not action_required


if __name__ == "__main__":
    dataset = import_data()

    # Remove employees with only "CENTRAL OFFICE" as their work location
    employees_to_remove = [
        emp_code for emp_code, emp in dataset.employees.items()
        if len(emp.work_areas) == 1 and next(iter(emp.work_areas)).location == "CENTRAL OFFICE"
    ]
    for emp_code in employees_to_remove:
        del dataset.employees[emp_code]

    report_generator = ReportGenerator(dataset)
    report_generator.process_employees()
