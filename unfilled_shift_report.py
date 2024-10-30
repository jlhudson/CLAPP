from collections import defaultdict
from datetime import datetime, timedelta
from math import ceil
from statistics import mean

from reportlogger import report_logger  # Ensure this imports your logger


class UnfilledShiftReport:
    ESCALATE_KEYWORDS = ["ESC", "ESCALATED"]
    READY_TO_ESCALATE_KEYWORDS = ["READY"]

    def __init__(self, dataset, default_lookahead_days=5):
        self.dataset = dataset
        self.default_lookahead_days = default_lookahead_days
        self.lookahead_days = self.calculate_lookahead_days()
        report_logger.info(f"Looking ahead {self.lookahead_days} days.")

    def calculate_lookahead_days(self):
        """Calculate lookahead days based on day of the week and time of day."""
        base_lookahead = self.default_lookahead_days
        today = datetime.now()
        day_of_week = today.weekday()  # Monday=0, Sunday=6
        hour = today.hour

        # Day-of-week adjustment: scales from 0 to 2 additional days by the end of the workweek
        day_of_week_factor = (day_of_week / 4) * 2  # Scale to add up to +2 by Friday

        # Time-of-day adjustment: add 30% if after 1:00 PM
        time_of_day_factor = 1.3 if hour >= 13 else 1

        # Final calculation with rounding up to ensure an integer
        adjusted_lookahead = base_lookahead + day_of_week_factor
        adjusted_lookahead *= time_of_day_factor
        return ceil(adjusted_lookahead)

    def generate_report(self):
        """Generate the upcoming shifts report, using the calculated lookahead period or the earliest grouping per location."""
        locations = {shift.work_area.location for shift in self.dataset.unassigned_shifts}

        for location in locations:
            self.process_location(location)

    def process_location(self, location):
        """Process and report unfilled shifts for a location, with the calculated lookahead period or fallback to the first available group."""
        today = datetime.now().date()
        lookahead_end_date = today + timedelta(days=self.lookahead_days)

        lookahead_shifts = [
            shift for shift in self.dataset.unassigned_shifts
            if shift.work_area.location == location and today <= shift.start.date() < lookahead_end_date
        ]

        if lookahead_shifts:
            self.print_headers()
            self.process_unfilled_shifts(lookahead_shifts)
        else:
            # If no shifts within the lookahead period, find the earliest grouping beyond that window
            later_shifts = [
                shift for shift in self.dataset.unassigned_shifts
                if shift.work_area.location == location and shift.start.date() >= lookahead_end_date
            ]
            if later_shifts:
                self.print_headers()
                first_group_start_date = min(shift.start.date() for shift in later_shifts)
                first_group = [shift for shift in later_shifts if shift.start.date() == first_group_start_date]
                self.process_unfilled_shifts(first_group)

    def process_unfilled_shifts(self, unfilled_shifts):
        """Process and output unfilled shifts, grouping linked and escalated shifts as needed, sorted by department average date."""
        shifts_by_loc_dept = defaultdict(list)
        for shift in unfilled_shifts:
            key = (shift.work_area.location, shift.work_area.department)
            shifts_by_loc_dept[key].append(shift)

        department_averages = []
        for (location, department), shifts in shifts_by_loc_dept.items():
            avg_date = mean([shift.start.timestamp() for shift in shifts])  # Calculate average date as timestamp
            department_averages.append(((location, department), avg_date))

        # Sort departments by average date and process each department group
        department_averages.sort(key=lambda x: x[1])  # Sort by average date (earliest first)

        group_id = 1
        for (location, department), _ in department_averages:
            sorted_shifts = sorted(shifts_by_loc_dept[(location, department)], key=lambda s: (s.start.date(), s.start.time()))
            groups = []
            shift_to_group = {}

            for i, shift in enumerate(sorted_shifts):
                priority = self.get_priority_label(shift.start.date())
                role = shift.work_area.role
                date_str = shift.start.strftime('%a %d/%m')
                start_end_str = f"{shift.start.strftime('%H%M')}-{shift.end.strftime('%H%M')}"
                status = self.get_escalation_status(shift.comment)
                linked = "Linked" if "Linked" in shift.comment else ""

                # Group by consecutive times within Location & Department
                potential_groups = [g for g in groups if sorted_shifts[g[-1]].end == shift.start]

                if potential_groups:
                    largest_group = max(potential_groups, key=len)
                    largest_group.append(i)
                    shift_to_group[i] = shift_to_group[largest_group[0]]
                else:
                    groups.append([i])
                    shift_to_group[i] = group_id
                    group_id += 1

                if linked and i not in shift_to_group:
                    cross_dept_shifts = [
                        (other_i, other_shift) for other_i, other_shift in enumerate(self.dataset.unassigned_shifts)
                        if other_shift.work_area.location == location and "Linked" in other_shift.comment and other_i not in shift_to_group
                    ]

                    if cross_dept_shifts:
                        cross_dept_group = max(cross_dept_shifts, key=lambda grp: len(grp[1]))
                        shift_to_group[i] = shift_to_group.get(cross_dept_group[0], group_id)
                        if cross_dept_group[0] not in shift_to_group:
                            shift_to_group[cross_dept_group[0]] = group_id
                            group_id += 1

                report_logger.clean(f"{priority}\t{location}\t{department}\t{role}\t{date_str}\t{start_end_str}\t{status}")

    def print_headers(self):
        """Print headers for each location report."""
        report_logger.clean("\nPriority\tLocation\tDepartment\tRole\tDate\tStart-End\tStatus")

    def get_priority_label(self, shift_date):
        """Determine priority label based on the shift date."""
        today = datetime.now().date()
        delta_days = (shift_date - today).days

        if delta_days == 0:
            return "Critical"
        elif delta_days <= 3:
            return "Urgent"
        elif delta_days <= self.lookahead_days:
            return "Upcoming"
        return ""

    def is_escalated_or_ready(self, shift):
        """Determine if a shift comment matches escalation or ready-to-escalate keywords."""
        comment = shift.comment.upper()
        return any(keyword in comment for keyword in self.ESCALATE_KEYWORDS) or \
            any(keyword in comment for keyword in self.READY_TO_ESCALATE_KEYWORDS)

    def get_escalation_status(self, comment):
        """Return the escalation status based on keywords in the comment."""
        comment_upper = comment.upper()
        if any(keyword in comment_upper for keyword in self.ESCALATE_KEYWORDS):
            return "Escalated"
        elif any(keyword in comment_upper for keyword in self.READY_TO_ESCALATE_KEYWORDS):
            return "Ready to Escalate"
        return ""
