
class GetInterventionKeyList(object):

    def __init__(self, conn, context):
        self._dw_connection = conn
        # self._dw_connection = DWAccess()
        self._context = context
        self._schema_name = self._context["SCHEMA_NAME"]

    def getting_intervention_key_list(self, type_list):
        """
        Return related intervention keys according to alert types from UI.

        :param type_list: Alert Types, e.g. 'IZS, Shelf OOS, Chronic OOS, DC Push' or '*'
        :return: key list: Getting related keys from table ANL_DIM_OSM_INTERVENTIONCLASSIFICATION. if * then return all keys
        """
        _list = self.__process(type_list)
        return _list

    def __process(self, type_list):
        print("\n-------------- Calling GetInterventionKeyList function --------------")
        print(type_list, type(type_list))
        _in_type_list = ','.join("\'" + ele + "\'" for ele in type_list.split(','))
        # print(_in_type_list)

        # if _type_list = '*', then retrieving all intervention keys.
        if type_list == '*':
            sql = "SELECT interventionkey FROM {SCHEMA_NAME}.ANL_DIM_OSM_INTERVENTIONCLASSIFICATION;"\
                .format(SCHEMA_NAME=self._schema_name)
            _intervention_keys = self._dw_connection.query_with_result(sql)

        # Otherwise, getting related intervention keys for given alert types.
        else:
            sql = "SELECT interventionkey FROM {SCHEMA_NAME}.ANL_DIM_OSM_INTERVENTIONCLASSIFICATION " \
                  "WHERE alerttype IN ({TYPE_LIST}) " \
                  "OR AlertSubType IN ({TYPE_LIST});".format(SCHEMA_NAME=self._schema_name, TYPE_LIST=_in_type_list)
            print(sql)
            _intervention_keys = self._dw_connection.query_with_result(sql)
            
        _intervention_key_list = ','.join([str(__tmp_key['INTERVENTIONKEY']) for __tmp_key in _intervention_keys])

        print("-------------- Calling GetInterventionKeyList function ended --------------\n")

        return _intervention_key_list


if __name__ == '__main__':
    get_key = GetInterventionKeyList('PEPSI_AHOLD_MB','TNV,D-Void,Shelf OOS,Phantom,Forced Count,IZS,AA','HUB_FUNCTION_MB')
    # get_key = GetInterventionKeyList('PEPSI_AHOLD_MB', '*', 'HUB_FUNCTION_MB')
    list = get_key.process()
    print(list)

