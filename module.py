
class Base:
    def __init__(self, ID= None, Name= None):
        self.__ID = ID
        self.__Name = Name
        
    @property
    def ID(self):
        if self.__ID != None:
            return self.__ID
        raise NotImplementedError()
    
    @property
    def Name(self):
        if self.__Name != None:
            return self.__Name
        raise NotImplementedError()
    
    @ID.setter
    def ID(self, ID):
        self.__ID = ID
        
    @Name.setter
    def Name(self, Name):
        self.__Name = Name

class URLBase(Base):
    def __init__(self, ID, Name, URL):
        super(URLBase, self).__init__(ID, Name)
        self.URL    = URL
        
class Industry(Base):
    def __init__(self, ID, Name):
        super(Industry, self).__init__(ID, Name)

class Category(URLBase):
    def __init__(self, ID, Name, URL, Industry: Industry):
        super(Category, self).__init__(ID, Name, URL)
        self.Industry = Industry
        
class Region(URLBase):
    def __init__(self, ID, Name, URL, Category: Category):
        super(Region, self).__init__(ID, Name, URL)
        self.Category = Category
        
class Location(URLBase):
    def __init__(self, ID, Name, URL, Region: Region):
        super(Location, self).__init__(ID, Name, URL)
        self.Region = Region
        
class Town(URLBase):
    def __init__(self, ID, Name, URL, Location: Location):
        super(Town, self).__init__(ID, Name, URL)
        self.Location = Location
        
class Company(Base):
    def __init__(self,ID: int, Name: str, Town: Town,
                 ShortName: str= "", SalesRevenue: str= "", CompanyType: str= "", Website: str= "",
                 Address: str= "", Phone: str= ""):
        super(Company, self).__init__(ID, Name)
        self.Town           = Town
        self.ShortName      = ShortName
        self.SalesRevenue   = SalesRevenue
        self.CompanyType    = CompanyType
        self.Website        = Website
        self.Address        = Address
        self.Phone          = Phone