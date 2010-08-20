var assert = require('assert');
include('./testconsts');
var {Store} = require('ngv/redisstore');
var store = new Store();
var personId, person, Person = store.defineEntity('Person');

exports.setUp = exports.tearDown = function() {
	for each (let instance in Person.all()) {
        instance.remove(); // Clean up.
    }
    assert.strictEqual(0, Person.all().length);
};


exports.testPersistCreation = function () {
    var person = createTestPerson();
    person.save();
    personId = person._id;
    person = Person.get(personId);
    assertPerson(person);
    assert.strictEqual(FIRST_NAME_1, person.firstName);
    assert.strictEqual(LAST_NAME, person.lastName);
    assert.deepEqual(new Date(BIRTH_DATE_MILLIS), person.birthDate);
    assert.strictEqual(BIRTH_YEAR, person.birthYear);
    assert.strictEqual(VITAE_1, person.vitae);
};


exports.testIdCounter = function () {

    var person1 = createTestPerson();
    var person2 = createTestPerson();

    person1.save();
    person2.save();

    assert.isNotNull(Person.get(1));
    assert.isNotNull(Person.get(2));

    assert.equal(Person.get(1)._id, 1);
    assert.equal(Person.get(2)._id, 2);

}

function createTestPerson() {
    return new Person({firstName: FIRST_NAME_1, lastName: LAST_NAME,
            birthDate: new Date(BIRTH_DATE_MILLIS), birthYear: BIRTH_YEAR,
            vitae: VITAE_1});
}



function assertPerson(person) {
    assert.isNotNull(person);
    assert.isTrue(person instanceof Storable &&
            person instanceof Person);
}
