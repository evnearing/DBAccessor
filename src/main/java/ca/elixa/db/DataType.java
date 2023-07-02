package ca.elixa.db;

public enum DataType {
    String,  Curve,

    Number, Decimal,

    Key, KeyList,

    //Maps
    StringKeyMap,
    StringStringMap,
    StringNumberMap,
    StringDecimalMap,

    EmbeddedEntity, EmbeddedEntityList
}
