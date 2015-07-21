import sys
import os.path
import csv


def usage():

    print('Usage:', sys.argv[0], 'filename', 'output')
    print('\n', '\t', 'filename:', 'name of file with input data')
    print('\n', '\t', 'output:', 'name of file to write output, e.g. results.txt')


def main():

    if len(sys.argv) < 3:
        usage()
        exit()

    filename = sys.argv[1]
    output = sys.argv[2]

    print('File:', filename)

    if os.path.isfile(filename) is False:
        print(filename, 'is not a file')
        exit()

    tenants = list()
    tenant_item_count = dict()
    tenant_total_results = dict()
    at_least_one_count = dict()
    at_least_five_count = dict()
    at_least_three_count = dict()

    with open(filename, 'r') as fp:
        reader = csv.reader(fp)

        next(reader)
        for row in reader:
            #'tenant','item_id','n_results','rtypes','rec_items','out_file'
            tenant, item_id, n_results, rtypes, rec_items = row

            if tenant not in tenants:
                tenants.append(tenant)

            if tenant not in tenant_item_count:
                tenant_item_count[tenant] = 1
            else:
                tenant_item_count[tenant] += 1
            if tenant not in tenant_total_results:
                tenant_total_results[tenant] = 0

            tenant_total_results[tenant] += int(n_results)

            if tenant not in at_least_one_count:
                at_least_one_count[tenant] = 0
            if tenant not in at_least_five_count:
                at_least_five_count[tenant] = 0
            if tenant not in at_least_three_count:
                at_least_three_count[tenant] = 0

            if int(n_results) >= 1:
                at_least_one_count[tenant] += 1
            if int(n_results) >= 5:
                at_least_five_count[tenant] += 1
            if int(n_results) >= 3:
                at_least_three_count[tenant] += 1

    msg = '\t'.join(['Tenant'.ljust(20), 'TotalItems'.ljust(10), 'AL-1-RSR'.ljust(10), 'AL-3-RSR'.ljust(10),
                     'AL-5-RSR'.ljust(10),
                     'AVG-RS'.ljust(10)])
    print(msg)

    with open(output, 'w') as fp:

        writer = csv.writer(fp, quoting=csv.QUOTE_ALL)
        writer.writerow(['Tenant'.ljust(20), 'TotalItems'.ljust(10), 'AL-1-RSR'.ljust(10), 'AL-3-RSR'.ljust(10),
                         'AL-5-RSR'.ljust(10), 'AVG-RS'.ljust(10)])

        for tenant in tenants:
            n = float(tenant_item_count[tenant])

            try:
                one_rate = at_least_one_count[tenant]/n
            except ZeroDivisionError:
                one_rate = 0

            try:
                five_rate = at_least_five_count[tenant]/n
            except ZeroDivisionError:
                five_rate = 0

            try:
                three_rate = at_least_three_count[tenant]/n
            except ZeroDivisionError:
                three_rate = 0

            try:
                avg_results = tenant_total_results[tenant]/n
            except ZeroDivisionError:
                avg_results = 0

            msg = '\t'.join([tenant.ljust(20), '{:,}'.format(n).ljust(10), '{0:.2%}'.format(one_rate).ljust(10),
                             '{0:.2%}'.format(three_rate).ljust(10),
                             '{0:.2%}'.format(five_rate).ljust(10),
                             '{0:.2}'.format(avg_results).ljust(10)])
            print(msg)

            writer.writerow([tenant.ljust(20), '{:,}'.format(n).ljust(10), '{0:.2%}'.format(one_rate).ljust(10),
                             '{0:.2%}'.format(three_rate).ljust(10),
                             '{0:.2%}'.format(five_rate).ljust(10),
                             '{0:.2}'.format(avg_results).ljust(10)])


if __name__ == '__main__':
    main()
