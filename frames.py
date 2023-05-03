import os
import customtkinter
from customtkinter import filedialog
import nifi_functions


class App(customtkinter.CTk):
    def __init__(self):
        super().__init__()
        self.title("GUI Example")
        self.geometry("400x250")

        self.step = 1

        # Step 1: Input URL
        self.url_label =  customtkinter.CTkLabel(self, text="Enter URL:")
        self.url_label.pack()
        self.url_entry = customtkinter.CTkEntry(self, width=10000)
        self.url_entry.pack()
        self.url_button = customtkinter.CTkButton(self, text="Next", command=self.handle_url)
        self.url_button.pack()

        # Step 2: Username and Password (Initially hidden)
        self.username_label = customtkinter.CTkLabel(self, text="Username:")
        self.username_entry = customtkinter.CTkEntry(self, width=10000)
        self.password_label = customtkinter.CTkLabel(self, text="Password:")
        self.password_entry = customtkinter.CTkEntry(self, width=10000, show="*")
        self.login_button = customtkinter.CTkButton(self, text="Next", command=self.handle_login)

        # Step 3: Choose File (Initially hidden)
        self.file_label = customtkinter.CTkLabel(self, text="Choose a file:")
        self.file_button = customtkinter.CTkButton(self, text="Browse", command=self.choose_file)

        # Step 4: Mapping Frame (Hidden initially)
        self.mapping_frame = None

    def handle_url(self):
        self.url_button.configure(state="disabled")  # Disable the button during the wait
        self.update_idletasks()  # Update the GUI to show the disabled button

        url = self.url_entry.get()
        print("url = ", url)
        if url:
            self.url_label.pack_forget()
            self.url_entry.pack_forget()
            self.url_button.pack_forget()

            # Perform the wait for the URL to be available 
            if nifi_functions.wait_for_endpoint_to_be_up(url):
                self.display_login_widgets()
            else:
                self.url_label.configure(text="URL not reachable. Try again.")
                self.url_label.pack()
                self.url_entry.pack()
                self.url_button.configure(state="normal")  # Enable the button again
                self.url_button.pack()
        else:
            self.url_button.configure(state="normal")  # Enable the button again


    def handle_login(self):
        self.login_button.configure(state="disabled")  # Disable the button during the wait
        self.update_idletasks()  # Update the GUI to show the disabled button

        username = self.username_entry.get()
        password = self.password_entry.get()
        if nifi_functions.login_nifi(username, password):  # Check if the credentials are valid
            self.username_label.pack_forget()
            self.username_entry.pack_forget()
            self.password_label.pack_forget()
            self.password_entry.pack_forget()
            self.login_button.pack_forget()
            self.display_file_selection_widgets()
        else:
            print("login a renvoyé False")
            self.username_label.configure(text="Invalid username or password. Please try again.")  # Show error message
        self.login_button.configure(state="normal")




    def choose_file(self):
        #TODO: verify the file type, if not database, display warningn and retry
        file_path = filedialog.askopenfilename()
        #TODO: look what file_path looks like, pass it to nifi in 
        if file_path:
            self.file_label.pack_forget()
            self.file_button.pack_forget()    
            # Show the Mapping Frame with the selected file path
            self.mapping_frame = MappingFrame(self, file_path)
            self.mapping_frame.pack()

    def display_login_widgets(self):
        self.step = 2
        self.username_label.pack()
        self.username_entry.pack()
        self.password_label.pack()
        self.password_entry.pack()
        self.login_button.pack()

    def display_file_selection_widgets(self):
        self.step = 3
        self.file_label.pack()
        self.file_button.pack()





class MappingFrame(customtkinter.CTkFrame):
    #TODO:necessary, why not juse apply the rules to the original cilulmn names directly
    def __init__(self, master, file_path):
        super().__init__(master)
        # Create and pack widgets for the mapping frame
        self.file_label = customtkinter.CTkLabel(self, text="Selected File:")
        self.file_label.pack()
        self.file_path_label = customtkinter.CTkLabel(self, text=file_path)
        self.file_path_label.pack()
        #separate file path and name
        self.parent_folder_path, self.filename = os.path.split(file_path)
        #TODO:recuperer le fichier
        #TODO:recuperer les noms de colonnes
        #TODO:proposer les noms de colonnes 
        #TODO:trouver un moyen d'effectuer le mapping





app = App()
app.mainloop()
