import flet as ft
from typing import List, Tuple


class PaginatedDataTable(ft.UserControl):
    # a default number of rows per page to be used in the data table
    DEFAULT_ROW_PER_PAGE = 10

    def __init__(
            self,
            datatable: ft.DataTable,
            rows_per_page: int = DEFAULT_ROW_PER_PAGE,
    ):
        """
        A customized user control which returns a paginated data table. It offers the possibility to organize data
        into pages and also define the number of rows to be shown on each page.

        :parameter datatable: a DataTable object to be used
        :parameter rows_per_page: the number of rows to be shown per page
        """
        super().__init__()

        self.dt = datatable
        self.rows_per_page = rows_per_page

        # number of rows in the table
        self.num_rows = len(datatable.rows)
        self.current_page = 1

        # Calculating the number of pages.
        p_int, p_add = divmod(self.num_rows, self.rows_per_page)
        self.num_pages = p_int + (1 if p_add else 0)

        # will display the current page number
        self.v_current_page = ft.Text(
            str(self.current_page),
            tooltip="Double click to set current page.",
            weight=ft.FontWeight.BOLD
        )

        # textfield to go to a particular page
        self.current_page_changer_field = ft.TextField(
            value=str(self.current_page),
            dense=True,
            filled=False,
            width=40,
            on_submit=lambda e: self.set_page(page=e.control.value),
            visible=False,
            keyboard_type=ft.KeyboardType.NUMBER,
            content_padding=2,
            text_align=ft.TextAlign.CENTER
        )

        # gesture detector to detect double taps of its contents
        self.gd = ft.GestureDetector(
            content=ft.Row(controls=[self.v_current_page, self.current_page_changer_field]),
            on_double_tap=self.on_double_tap_page_changer,
        )

        # textfield to change the number of rows_per_page
        self.v_num_of_row_changer_field = ft.TextField(
            value=str(self.rows_per_page),
            dense=True,
            filled=False,
            width=40,
            on_submit=lambda e: self.set_rows_per_page(e.control.value),
            keyboard_type=ft.KeyboardType.NUMBER,
            content_padding=2,
            text_align=ft.TextAlign.CENTER
        )

        # will display the number of rows in the table
        self.v_count = ft.Text(weight=ft.FontWeight.BOLD)

        self.pdt = ft.DataTable(
            columns=self.dt.columns,
            rows=self.build_rows()
        )

    @property
    def datatable(self) -> ft.DataTable:
        return self.pdt

    @property
    def datacolumns(self) -> List[ft.DataColumn]:
        return self.pdt.columns

    @property
    def datarows(self) -> List[ft.DataRow]:
        return self.dt.rows

    def set_rows_per_page(self, new_row_per_page: str):
        """
        Takes a string as an argument, tries converting it to an integer, and sets the number of rows per page to that
        integer if it is between 1 and the total number of rows, otherwise it sets the number of rows per page to the
        default value

        :param new_row_per_page: The new number of rows per page
        :type new_row_per_page: str
        :raise ValueError
        """
        try:
            self.rows_per_page = int(new_row_per_page) \
                if 1 <= int(new_row_per_page) <= self.num_rows \
                else self.DEFAULT_ROW_PER_PAGE
        except ValueError:
            # if an error occurs set to default
            self.rows_per_page = self.DEFAULT_ROW_PER_PAGE
        self.v_num_of_row_changer_field.value = str(self.rows_per_page)

        # Calculating the number of pages.
        p_int, p_add = divmod(self.num_rows, self.rows_per_page)
        self.num_pages = p_int + (1 if p_add else 0)

        self.set_page(page=1)
        # self.refresh_data()

    def set_page(self, page: [str, int, None] = None, delta: int = 0):
        """
        Sets the current page using the page parameter if provided. Else if the delta is not 0,
        sets the current page to the current page plus the provided delta.

        :param page: the page number to display
        :param delta: The number of pages to move forward or backward, defaults to 0 (optional)
        :return: The current page number.
        :raise ValueError
        """
        if page is not None:
            try:
                self.current_page = int(page) if 1 <= int(page) <= self.num_pages else 1
            except ValueError:
                self.current_page = 1
        elif delta:
            self.current_page += delta
        else:
            return
        self.refresh_data()

    def next_page(self, e: ft.ControlEvent):
        """sets the current page to the next page"""
        if self.current_page < self.num_pages:
            self.set_page(delta=1)

    def prev_page(self, e: ft.ControlEvent):
        """set the current page to the previous page"""
        if self.current_page > 1:
            self.set_page(delta=-1)

    def goto_first_page(self, e: ft.ControlEvent):
        """sets the current page to the first page"""
        self.set_page(page=1)

    def goto_last_page(self, e: ft.ControlEvent):
        """sets the current page to the last page"""
        self.set_page(page=self.num_pages)

    def build_rows(self) -> list:
        """
        Returns a slice of indexes, using the start and end values returned by the paginate() function
        :return: The rows of data that are being displayed on the page.
        """
        return self.dt.rows[slice(*self.paginate())]

    def paginate(self) -> Tuple[int, int]:
        """
        Returns a tuple of two integers, where the first is the index of the first row to be displayed
        on the current page, and `the second the index of the last row to be displayed on the current page
        :return: A tuple of two integers.
        """
        i1_multiplier = 0 if self.current_page == 1 else self.current_page - 1
        i1 = i1_multiplier * self.rows_per_page
        i2 = self.current_page * self.rows_per_page

        return i1, i2

    def build(self):
        return ft.Container(
            ft.Column(
                [
                    ft.Container(
                        self.pdt,
                        alignment=ft.alignment.center
                    ),
                    ft.Row(
                        [
                            ft.Row(
                                controls=[
                                    ft.IconButton(
                                        ft.icons.KEYBOARD_DOUBLE_ARROW_LEFT,
                                        on_click=self.goto_first_page,
                                        tooltip="First Page"
                                    ),
                                    ft.IconButton(
                                        ft.icons.KEYBOARD_ARROW_LEFT,
                                        on_click=self.prev_page,
                                        tooltip="Previous Page"
                                    ),
                                    self.gd,
                                    ft.IconButton(
                                        ft.icons.KEYBOARD_ARROW_RIGHT,
                                        on_click=self.next_page,
                                        tooltip="Next Page"
                                    ),
                                    ft.IconButton(
                                        ft.icons.KEYBOARD_DOUBLE_ARROW_RIGHT,
                                        on_click=self.goto_last_page,
                                        tooltip="Last Page"
                                    ),
                                ]
                            ),
                            ft.Row(
                                controls=[
                                    self.v_num_of_row_changer_field, ft.Text("rows per page")
                                ]
                            ),
                            self.v_count,
                        ],
                        alignment=ft.MainAxisAlignment.SPACE_EVENLY
                    ),
                ],
                scroll=ft.ScrollMode.AUTO,
                alignment=ft.MainAxisAlignment.CENTER
            ),
            padding=10,
            alignment=ft.alignment.center
        )

    def on_double_tap_page_changer(self, e):
        """
        Called when the content of the GestureDetector (gd) is double tapped.
        Toggles the visibility of gd's content.
        """
        self.current_page_changer_field.value = str(self.current_page)
        self.v_current_page.visible = not self.v_current_page.visible
        self.current_page_changer_field.visible = not self.current_page_changer_field.visible
        self.update()

    def refresh_data(self):
        # Setting the rows of the paginated datatable to the rows returned by the `build_rows()` function.
        self.pdt.rows = self.build_rows()

        self.num_rows = len(self.dt.rows)
        p_int, p_add = divmod(self.num_rows, self.rows_per_page)
        self.num_pages = p_int + (1 if p_add else 0)

        # display the total number of rows in the table.
        self.v_count.value = f"Total Rows: {self.num_rows}"
        # the current page number versus the total number of pages.
        self.v_current_page.value = f"{self.current_page}/{self.num_pages}"

        # update the visibility of controls in the gesture detector
        self.current_page_changer_field.visible = False
        self.v_current_page.visible = True

        # update the control so the above changes are rendered in the UI
        self.update()

    def did_mount(self):
        self.refresh_data()
