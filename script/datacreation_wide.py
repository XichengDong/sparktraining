import sys, getopt
from faker import Factory
#----------------------------------------------------------------------
def main(argv):
    """"""
    delm = '|'
    num_rows = 4000000
    try:
        opts, args = getopt.getopt(argv,"r:",["rows="])
    except getopt.GetoptError:
        print 'Usage: datacreation1.py -r "number_rows"'
        sys.exit(2)
    for opt, arg in opts:
        if opt in("-r", "--rows" ):
            num_rows = arg
        else:
            assert False, "unhandled option"
    fake = Factory.create()
    create_data(fake,delm,num_rows)

def create_data(fake,delm,num_rows):
    """"""
    for i in range(int(num_rows)):
        record = ""
        firstRecord = True

        # Add some user data at the beginning of each record
        record += fake.first_name() \
        + delm + fake.last_name() \
        + delm + fake.email() \
        + delm + fake.company() \
        + delm + fake.job() \
        + delm + fake.street_address() \
        + delm + fake.city() \
        + delm + fake.state_abbr() \
        + delm + fake.zipcode_plus4() \
        + delm + fake.url() \
        + delm + fake.phone_number() \
        + delm + fake.user_agent() \
        + delm + fake.user_name() \

        # Repeats the same 3 columns 329 times = 987 cols + 13 initial = 1000
        for j in range(329):
            record += delm + fake.random_letter() \
            + delm + str(fake.random_int(min=0,max=9999)) \
            + delm + str(fake.boolean(chance_of_getting_true=50))
       print record

if __name__ == "__main__":
    main(sys.argv[1:])